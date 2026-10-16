package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"backend/internal/middleware"
	"backend/internal/orchestrator"
	"backend/internal/plane"
	"backend/internal/sandboxfiles"
)

// FileExecer runs the files API's guest scripts on the sandbox's worker
// without resuming a paused sandbox (controlplane.Service.ExecIfActive).
type FileExecer interface {
	ExecIfActive(ctx context.Context, sandboxID, command string, timeoutSec int, opts plane.ExecOptions) (plane.ExecResult, error)
}

// maxFileWriteBodyBytes fits a 2 MiB file whose every byte is JSON-escaped
// as \u00XX, plus the other fields.
const maxFileWriteBodyBytes = 6*sandboxfiles.MaxFileBytes + 64<<10

// WriteFileRequest is the body of PUT /v1/sandboxes/{id}/files/content.
// ExpectedSHA256 is the sha256 the editor loaded; null (or absent) creates a
// new file.
type WriteFileRequest struct {
	Path           string  `json:"path"`
	Content        *string `json:"content"`
	ExpectedSHA256 *string `json:"expected_sha256"`
}

// fileError is the files API's error body: the usual envelope, plus size for
// file_too_large and the current sha256 (null: the file is gone) for conflict.
type fileError struct {
	APIError
	Size   int64           `json:"size,omitempty"`
	SHA256 json.RawMessage `json:"sha256,omitempty"`
}

var fileErrorStatus = map[string]int{
	sandboxfiles.CodeInvalidPath:    http.StatusBadRequest,
	sandboxfiles.CodeInvalidRev:     http.StatusBadRequest,
	sandboxfiles.CodeInvalidSHA256:  http.StatusBadRequest,
	sandboxfiles.CodeNotADirectory:  http.StatusBadRequest,
	sandboxfiles.CodeNotAFile:       http.StatusBadRequest,
	sandboxfiles.CodeNotAGitRepo:    http.StatusBadRequest,
	sandboxfiles.CodeNotFound:       http.StatusNotFound,
	sandboxfiles.CodeFileNotFound:   http.StatusNotFound,
	sandboxfiles.CodeParentNotFound: http.StatusNotFound,
	sandboxfiles.CodeRevNotFound:    http.StatusNotFound,
	sandboxfiles.CodeNotInRevision:  http.StatusNotFound,
	sandboxfiles.CodeFileTooLarge:   http.StatusRequestEntityTooLarge,
	sandboxfiles.CodeConflict:       http.StatusConflict,
	sandboxfiles.CodeExists:         http.StatusConflict,
	sandboxfiles.CodeGuestFailed:    http.StatusBadGateway,
}

// FileTreeHandler serves GET /v1/sandboxes/{sandboxID}/files/tree?root=.
func FileTreeHandler(sandboxes plane.Service, execer FileExecer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, ok := openFiles(w, r, sandboxes, execer)
		if !ok {
			return
		}
		tree, err := req.files.Tree(r.Context(), r.URL.Query().Get("root"))
		if err != nil {
			req.fail(w, "tree", err)
			return
		}
		writeFilesJSON(w, http.StatusOK, tree)
	}
}

// ReadFileHandler serves GET /v1/sandboxes/{sandboxID}/files/content?path=[&rev=].
func ReadFileHandler(sandboxes plane.Service, execer FileExecer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, ok := openFiles(w, r, sandboxes, execer)
		if !ok {
			return
		}
		query := r.URL.Query()
		file, err := req.files.Read(r.Context(), query.Get("path"), query.Get("rev"))
		if err != nil {
			req.fail(w, "read", err)
			return
		}
		writeFilesJSON(w, http.StatusOK, file)
	}
}

// WriteFileHandler serves PUT /v1/sandboxes/{sandboxID}/files/content.
func WriteFileHandler(sandboxes plane.Service, execer FileExecer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, ok := openFiles(w, r, sandboxes, execer)
		if !ok {
			return
		}
		var body WriteFileRequest
		if err := decodeFileWrite(w, r, &body); err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				req.fail(w, "write", &sandboxfiles.Error{Code: sandboxfiles.CodeFileTooLarge, Message: "file is larger than 2 MiB"})
				return
			}
			writeTerminalError(w, http.StatusBadRequest, "invalid_request", "body must be {path, content, expected_sha256}", req.requestID)
			return
		}
		if body.Content == nil {
			writeTerminalError(w, http.StatusBadRequest, "invalid_request", "content is required", req.requestID)
			return
		}
		written, err := req.files.Write(r.Context(), body.Path, []byte(*body.Content), body.ExpectedSHA256)
		if err != nil {
			req.fail(w, "write", err)
			return
		}
		writeFilesJSON(w, http.StatusOK, written)
	}
}

// FileChangesHandler serves GET /v1/sandboxes/{sandboxID}/files/changes?root=&base=.
func FileChangesHandler(sandboxes plane.Service, execer FileExecer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, ok := openFiles(w, r, sandboxes, execer)
		if !ok {
			return
		}
		query := r.URL.Query()
		changes, err := req.files.Changes(r.Context(), query.Get("root"), query.Get("base"))
		if err != nil {
			req.fail(w, "changes", err)
			return
		}
		writeFilesJSON(w, http.StatusOK, changes)
	}
}

// filesRequest is one authorized files API request.
type filesRequest struct {
	files     *sandboxfiles.Client
	requestID string
	sandboxID string
}

// openFiles authorizes the request: the caller must own the sandbox (404
// otherwise) and it must be active (409, never resumed here). The returned
// client runs scripts on the sandbox's worker.
func openFiles(w http.ResponseWriter, r *http.Request, sandboxes plane.Service, execer FileExecer) (filesRequest, bool) {
	requestID := middleware.RequestIDFromContext(r.Context())
	auth, ok := middleware.AuthFromContext(r.Context())
	if !ok {
		writeTerminalError(w, http.StatusUnauthorized, "unauthorized", "authentication is required", requestID)
		return filesRequest{}, false
	}
	sandboxID := r.PathValue("sandboxID")
	sandbox, found := sandboxes.GetSession(r.Context(), sandboxID)
	if !found || sandbox.UserID != auth.TenantID {
		writeTerminalError(w, http.StatusNotFound, "sandbox_not_found", "sandbox not found", requestID)
		return filesRequest{}, false
	}
	if sandbox.State != plane.StateActive {
		writeTerminalError(w, http.StatusConflict, "sandbox_not_active", "sandbox must be active to read or write files", requestID)
		return filesRequest{}, false
	}
	exec := func(ctx context.Context, command string, timeoutS int, opts plane.ExecOptions) (plane.ExecResult, error) {
		return execer.ExecIfActive(ctx, sandboxID, command, timeoutS, opts)
	}
	log := slog.With("component", "files", "request_id", requestID, "sandbox_id", sandboxID)
	return filesRequest{files: sandboxfiles.New(exec, log), requestID: requestID, sandboxID: sandboxID}, true
}

// fail writes the error for a failed files operation.
func (req filesRequest) fail(w http.ResponseWriter, op string, err error) {
	var fe *sandboxfiles.Error
	switch {
	case errors.As(err, &fe):
		status, known := fileErrorStatus[fe.Code]
		if !known {
			status = http.StatusBadGateway
		}
		body := fileError{APIError: APIError{Status: "error", Code: fe.Code, Message: fe.Message, RequestID: req.requestID}}
		switch fe.Code {
		case sandboxfiles.CodeFileTooLarge:
			body.Size = fe.Size
		case sandboxfiles.CodeConflict:
			body.SHA256 = json.RawMessage("null")
			if fe.SHA256 != nil {
				raw, marshalErr := json.Marshal(*fe.SHA256)
				if marshalErr == nil {
					body.SHA256 = raw
				}
			}
		}
		if status >= http.StatusInternalServerError {
			slog.Warn("files request failed", "op", op, "request_id", req.requestID, "sandbox_id", req.sandboxID, "code", fe.Code)
		}
		writeFilesJSON(w, status, body)
	case errors.Is(err, plane.ErrSandboxNotActive):
		writeTerminalError(w, http.StatusConflict, "sandbox_not_active", "sandbox must be active to read or write files", req.requestID)
	case errors.Is(err, orchestrator.ErrSandboxNotFound):
		writeTerminalError(w, http.StatusConflict, "sandbox_not_placed", "sandbox worker placement is unavailable", req.requestID)
	default:
		slog.Error("files request failed", "op", op, "request_id", req.requestID, "sandbox_id", req.sandboxID, "err", err)
		writeTerminalError(w, http.StatusBadGateway, sandboxfiles.CodeGuestFailed, "the sandbox could not complete the files request", req.requestID)
	}
}

func decodeFileWrite(w http.ResponseWriter, r *http.Request, dst *WriteFileRequest) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxFileWriteBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("request body must contain one JSON object")
	}
	return nil
}

func writeFilesJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Warn("writing files response failed", "err", err)
	}
}
