package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"backend/internal/plane"
	"backend/internal/sandboxfiles"
	"backend/internal/uploads"
)

// UploadStore keeps pending uploads (uploads.Service). A nil store means
// uploads are not configured on this server.
type UploadStore interface {
	MaxBytes() int64
	Create(sandboxID, userID string, size int64, sum string) (uploads.Created, error)
	Begin(ctx context.Context, id, sandboxID, userID string) (uploads.Ticket, error)
	Finish(ctx context.Context, id string, discard bool)
}

// CreateUploadRequest is the body of POST /v1/sandboxes/{id}/uploads: the
// size and sha256 of the .tar.gz the client will PUT.
type CreateUploadRequest struct {
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// CreateUploadResponse says where to PUT the archive, and until when.
type CreateUploadResponse struct {
	Status     string            `json:"status"`
	RequestID  string            `json:"request_id"`
	UploadID   string            `json:"upload_id"`
	PutURL     string            `json:"put_url"`
	PutHeaders map[string]string `json:"put_headers"`
	ExpiresAt  time.Time         `json:"expires_at"`
	MaxBytes   int64             `json:"max_bytes"`
}

// ExtractUploadRequest is the body of POST .../uploads/{uploadID}/extract.
type ExtractUploadRequest struct {
	Dest string `json:"dest"`
}

// ExtractUploadResponse is a finished extract.
type ExtractUploadResponse struct {
	Status    string `json:"status"`
	RequestID string `json:"request_id"`
	UploadID  string `json:"upload_id"`
	sandboxfiles.Extracted
}

// uploadError is an upload error body; max_bytes comes with upload_too_large.
type uploadError struct {
	APIError
	MaxBytes int64 `json:"max_bytes,omitempty"`
}

// extractTimeout bounds one extract's wait for the guest (its script has its
// own, shorter limits); the HTTP client's disconnect does not cut it short,
// so the upload is always finished.
const extractTimeout = 20 * time.Minute

// CreateUploadHandler serves POST /v1/sandboxes/{sandboxID}/uploads. The
// caller must own the sandbox (404) and it must be active (409).
func CreateUploadHandler(sandboxes plane.Service, execer FileExecer, store UploadStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, ok := openFiles(w, r, sandboxes, execer)
		if !ok {
			return
		}
		if store == nil {
			writeTerminalError(w, http.StatusServiceUnavailable, "uploads_unavailable", "uploads are not configured on this server", req.requestID)
			return
		}
		var body CreateUploadRequest
		if err := decodeJSON(w, r, &body); err != nil {
			writeTerminalError(w, http.StatusBadRequest, "invalid_request", "body must be {size, sha256}", req.requestID)
			return
		}
		created, err := store.Create(req.sandboxID, req.userID, body.Size, body.SHA256)
		if err != nil {
			status, code := http.StatusInternalServerError, "upload_failed"
			message := "failed to create the upload"
			errBody := uploadError{}
			switch {
			case errors.Is(err, uploads.ErrInvalidSize):
				status, code, message = http.StatusBadRequest, "invalid_size", err.Error()
			case errors.Is(err, uploads.ErrTooLarge):
				status, code, message = http.StatusRequestEntityTooLarge, "upload_too_large", err.Error()
				errBody.MaxBytes = store.MaxBytes()
			case errors.Is(err, uploads.ErrInvalidSHA256):
				status, code, message = http.StatusBadRequest, "invalid_sha256", err.Error()
			case errors.Is(err, uploads.ErrTooMany):
				status, code, message = http.StatusTooManyRequests, "too_many_uploads", err.Error()
			default:
				slog.Error("creating an upload failed", "request_id", req.requestID, "sandbox_id", req.sandboxID, "err", err)
			}
			errBody.APIError = APIError{Status: "error", Code: code, Message: message, RequestID: req.requestID}
			writeFilesJSON(w, status, errBody)
			return
		}
		slog.Info("upload created", "request_id", req.requestID, "sandbox_id", req.sandboxID,
			"upload_id", created.ID, "size", created.Size)
		writeFilesJSON(w, http.StatusCreated, CreateUploadResponse{
			Status:     "success",
			RequestID:  req.requestID,
			UploadID:   created.ID,
			PutURL:     created.PutURL,
			PutHeaders: uploads.PutHeaders,
			ExpiresAt:  created.ExpiresAt.UTC(),
			MaxBytes:   store.MaxBytes(),
		})
	}
}

// ExtractUploadHandler serves POST /v1/sandboxes/{sandboxID}/uploads/{uploadID}/extract:
// the sandbox downloads the upload and unpacks it at dest
// (sandboxfiles.Extract). A successful extract, or an archive the sandbox
// refuses, deletes the upload; anything else (dest in use, the download, the
// sandbox) leaves it to retry until it expires.
func ExtractUploadHandler(sandboxes plane.Service, execer FileExecer, store UploadStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, ok := openFiles(w, r, sandboxes, execer)
		if !ok {
			return
		}
		if store == nil {
			writeTerminalError(w, http.StatusServiceUnavailable, "uploads_unavailable", "uploads are not configured on this server", req.requestID)
			return
		}
		var body ExtractUploadRequest
		if err := decodeJSON(w, r, &body); err != nil || body.Dest == "" {
			writeTerminalError(w, http.StatusBadRequest, "invalid_request", "body must be {dest}", req.requestID)
			return
		}
		if err := req.files.CheckDest(body.Dest); err != nil {
			req.fail(w, "extract", err)
			return
		}
		uploadID := r.PathValue("uploadID")
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), extractTimeout)
		defer cancel()
		ticket, err := store.Begin(ctx, uploadID, req.sandboxID, req.userID)
		if err != nil {
			writeBeginError(w, req, uploadID, err)
			return
		}
		extracted, err := req.files.Extract(ctx, sandboxfiles.ExtractRequest{
			URL: ticket.GetURL, SHA256: ticket.SHA256, Size: ticket.Size, Dest: body.Dest,
		})
		if err != nil {
			var fe *sandboxfiles.Error
			discard := errors.As(err, &fe) && sandboxfiles.ArchiveRejected(fe.Code)
			store.Finish(ctx, uploadID, discard)
			slog.Warn("extracting an upload failed", "request_id", req.requestID, "sandbox_id", req.sandboxID,
				"upload_id", uploadID, "discarded", discard, "err", err)
			req.fail(w, "extract", err)
			return
		}
		store.Finish(ctx, uploadID, true)
		slog.Info("upload extracted", "request_id", req.requestID, "sandbox_id", req.sandboxID,
			"upload_id", uploadID, "entries", extracted.Entries, "bytes", extracted.Bytes)
		writeFilesJSON(w, http.StatusOK, ExtractUploadResponse{
			Status: "success", RequestID: req.requestID, UploadID: uploadID, Extracted: extracted,
		})
	}
}

func writeBeginError(w http.ResponseWriter, req filesRequest, uploadID string, err error) {
	switch {
	case errors.Is(err, uploads.ErrNotFound):
		writeTerminalError(w, http.StatusNotFound, "upload_not_found", "upload not found", req.requestID)
	case errors.Is(err, uploads.ErrExpired):
		writeTerminalError(w, http.StatusGone, "upload_expired", "the upload expired; upload again", req.requestID)
	case errors.Is(err, uploads.ErrBusy):
		writeTerminalError(w, http.StatusConflict, "upload_busy", "the upload is already being extracted", req.requestID)
	case errors.Is(err, uploads.ErrIncomplete):
		writeTerminalError(w, http.StatusConflict, "upload_incomplete", "the archive has not been uploaded to put_url yet", req.requestID)
	case errors.Is(err, uploads.ErrSizeMismatch):
		writeTerminalError(w, http.StatusUnprocessableEntity, sandboxfiles.CodeSizeMismatch, "the uploaded archive's size is not the declared size", req.requestID)
	default:
		slog.Error("starting an extract failed", "request_id", req.requestID, "sandbox_id", req.sandboxID, "upload_id", uploadID, "err", err)
		writeTerminalError(w, http.StatusBadGateway, "upload_storage_failed", "upload storage is unavailable", req.requestID)
	}
}

// Compile-time check that the uploads service satisfies the store.
var _ UploadStore = (*uploads.Service)(nil)
