// Package uploads lets a client (the spawn CLI) send a folder to a sandbox
// without streaming it through the API: the control plane hands out a
// short-lived, write-only SAS URL for one new blob, the client PUTs a .tar.gz
// there, and on extract the sandbox downloads it through a short-lived,
// read-only SAS URL (sandboxfiles.Extract). The blob is deleted once it is
// extracted, when it turns out unusable, or when it expires.
//
// Records live in memory: the control plane is a single process, and nothing
// about an upload outlives its 15 minutes. A restart forgets pending uploads
// (the client uploads again); their blobs are swept by age.
package uploads

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	// DefaultMaxBytes caps one upload unless configured otherwise.
	DefaultMaxBytes = 500 << 20
	// LimitMaxBytes is the most the cap may be: one Put Blob request.
	LimitMaxBytes = 5000 << 20
	// PutTTL is how long an upload (and its write URL) is valid; extract
	// must start before it ends.
	PutTTL = 15 * time.Minute
	// GetTTL is the read URL's lifetime: the guest starts downloading at once.
	GetTTL = 10 * time.Minute
	// Prefix is where upload blobs live in the container.
	Prefix = "uploads/"
	// MaxPendingPerSandbox bounds unextracted uploads per sandbox.
	MaxPendingPerSandbox = 8

	sweepInterval = 5 * time.Minute
	// orphanAge: an upload blob is in use for at most PutTTL plus one
	// extract, so any older one was left by a restart.
	orphanAge = time.Hour
	// clockSkew backdates SAS start times.
	clockSkew   = 5 * time.Minute
	blobTimeout = 30 * time.Second
)

// PutHeaders must be sent with the PUT to the write URL.
var PutHeaders = map[string]string{"x-ms-blob-type": "BlockBlob"}

var (
	ErrInvalidSize   = errors.New("size must be a positive number of bytes")
	ErrTooLarge      = errors.New("upload is larger than the limit")
	ErrInvalidSHA256 = errors.New("sha256 must be 64 lower-case hex characters")
	ErrTooMany       = errors.New("too many pending uploads for this sandbox")
	ErrNotFound      = errors.New("upload not found")
	ErrExpired       = errors.New("upload expired")
	ErrBusy          = errors.New("upload is being extracted")
	ErrIncomplete    = errors.New("the upload's blob has not been uploaded yet")
	ErrSizeMismatch  = errors.New("the uploaded blob's size is not the declared size")
)

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Blobs is the blob storage uploads use (AzureBlobs).
type Blobs interface {
	// PutURL and GetURL sign write-only and read-only URLs for one blob.
	PutURL(name string, expiry time.Time) (string, error)
	GetURL(name string, expiry time.Time) (string, error)
	// Size is the blob's size; false when it does not exist.
	Size(ctx context.Context, name string) (int64, bool, error)
	// Delete removes a blob; a missing blob is not an error.
	Delete(ctx context.Context, name string) error
	// ListOlder lists the blobs under prefix last modified before t.
	ListOlder(ctx context.Context, prefix string, t time.Time) ([]string, error)
}

// Upload is one pending upload.
type Upload struct {
	ID        string
	SandboxID string
	UserID    string
	Size      int64
	SHA256    string
	Blob      string
	ExpiresAt time.Time
}

// Created is a new upload and where to PUT it.
type Created struct {
	Upload
	PutURL string
}

// Ticket is an upload ready to extract and where to download it.
type Ticket struct {
	Upload
	GetURL string
}

type record struct {
	Upload
	busy bool
}

// Service keeps pending uploads.
type Service struct {
	blobs    Blobs
	maxBytes int64
	log      *slog.Logger
	now      func() time.Time

	mu      sync.Mutex
	uploads map[string]*record
}

// New returns a service that stores uploads in blobs, each at most maxBytes.
func New(blobs Blobs, maxBytes int64, log *slog.Logger) *Service {
	return &Service{blobs: blobs, maxBytes: maxBytes, log: log, now: time.Now, uploads: map[string]*record{}}
}

// MaxBytes is the largest upload accepted.
func (s *Service) MaxBytes() int64 {
	return s.maxBytes
}

// Create records an upload of size bytes with the given sha256 for the
// caller's sandbox and signs its write URL.
func (s *Service) Create(sandboxID, userID string, size int64, sum string) (Created, error) {
	if size <= 0 {
		return Created{}, ErrInvalidSize
	}
	if size > s.maxBytes {
		return Created{}, ErrTooLarge
	}
	if !sha256Pattern.MatchString(sum) {
		return Created{}, ErrInvalidSHA256
	}
	now := s.now()
	id := uuid.NewString()
	upload := Upload{
		ID: id, SandboxID: sandboxID, UserID: userID, Size: size, SHA256: sum,
		Blob: Prefix + id + ".tar.gz", ExpiresAt: now.Add(PutTTL),
	}
	putURL, err := s.blobs.PutURL(upload.Blob, upload.ExpiresAt)
	if err != nil {
		return Created{}, fmt.Errorf("sign upload URL: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	pending := 0
	for _, r := range s.uploads {
		if r.SandboxID == sandboxID && now.Before(r.ExpiresAt) {
			pending++
		}
	}
	if pending >= MaxPendingPerSandbox {
		return Created{}, ErrTooMany
	}
	s.uploads[id] = &record{Upload: upload}
	return Created{Upload: upload, PutURL: putURL}, nil
}

// Begin claims an upload for extraction: it must be the caller's, for this
// sandbox, unexpired, not already being extracted, and fully uploaded with
// the declared size (a blob of another size is deleted). Every successful
// Begin must be followed by Finish.
func (s *Service) Begin(ctx context.Context, id, sandboxID, userID string) (Ticket, error) {
	now := s.now()
	s.mu.Lock()
	r, ok := s.uploads[id]
	if !ok || r.SandboxID != sandboxID || r.UserID != userID {
		s.mu.Unlock()
		return Ticket{}, ErrNotFound
	}
	if !now.Before(r.ExpiresAt) && !r.busy {
		delete(s.uploads, id)
		s.mu.Unlock()
		s.deleteBlob(ctx, r.Blob)
		return Ticket{}, ErrExpired
	}
	if r.busy {
		s.mu.Unlock()
		return Ticket{}, ErrBusy
	}
	r.busy = true
	upload := r.Upload
	s.mu.Unlock()

	size, exists, err := s.blobs.Size(ctx, upload.Blob)
	switch {
	case err != nil:
		s.Finish(ctx, id, false)
		return Ticket{}, fmt.Errorf("check upload blob: %w", err)
	case !exists:
		s.Finish(ctx, id, false)
		return Ticket{}, ErrIncomplete
	case size != upload.Size:
		s.Finish(ctx, id, true)
		return Ticket{}, ErrSizeMismatch
	}
	getURL, err := s.blobs.GetURL(upload.Blob, now.Add(GetTTL))
	if err != nil {
		s.Finish(ctx, id, false)
		return Ticket{}, fmt.Errorf("sign download URL: %w", err)
	}
	return Ticket{Upload: upload, GetURL: getURL}, nil
}

// Finish ends an extraction that Begin claimed. With discard (extracted, or
// unusable) the upload and its blob are deleted; otherwise it can be
// extracted again until it expires.
func (s *Service) Finish(ctx context.Context, id string, discard bool) {
	s.mu.Lock()
	r, ok := s.uploads[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	if !discard {
		r.busy = false
		s.mu.Unlock()
		return
	}
	delete(s.uploads, id)
	s.mu.Unlock()
	s.deleteBlob(ctx, r.Blob)
}

// Run sweeps expired uploads and orphaned blobs until ctx ends.
func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for {
		s.Sweep(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Sweep deletes expired uploads (not being extracted) with their blobs, and
// upload blobs older than orphanAge that no upload owns.
func (s *Service) Sweep(ctx context.Context) {
	now := s.now()
	var expired []string
	live := map[string]bool{}
	s.mu.Lock()
	for id, r := range s.uploads {
		if !now.Before(r.ExpiresAt) && !r.busy {
			expired = append(expired, r.Blob)
			delete(s.uploads, id)
			continue
		}
		live[r.Blob] = true
	}
	s.mu.Unlock()
	for _, blob := range expired {
		s.deleteBlob(ctx, blob)
	}

	listCtx, cancel := context.WithTimeout(ctx, blobTimeout)
	orphans, err := s.blobs.ListOlder(listCtx, Prefix, now.Add(-orphanAge))
	cancel()
	if err != nil {
		s.log.Warn("listing old upload blobs failed", "err", err)
		return
	}
	for _, blob := range orphans {
		if !live[blob] {
			s.deleteBlob(ctx, blob)
		}
	}
}

func (s *Service) deleteBlob(ctx context.Context, blob string) {
	deleteCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), blobTimeout)
	defer cancel()
	if err := s.blobs.Delete(deleteCtx, blob); err != nil {
		s.log.Warn("deleting an upload blob failed; the sweep retries it", "blob", blob, "err", err)
	}
}
