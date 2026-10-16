package uploads

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeBlobs struct {
	mu       sync.Mutex
	sizes    map[string]int64
	modified map[string]time.Time
	deleted  []string
	sizeErr  error
}

func newFakeBlobs() *fakeBlobs {
	return &fakeBlobs{sizes: map[string]int64{}, modified: map[string]time.Time{}}
}

func (f *fakeBlobs) PutURL(name string, _ time.Time) (string, error) { return "put:" + name, nil }
func (f *fakeBlobs) GetURL(name string, _ time.Time) (string, error) { return "get:" + name, nil }

func (f *fakeBlobs) Size(_ context.Context, name string) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sizeErr != nil {
		return 0, false, f.sizeErr
	}
	size, ok := f.sizes[name]
	return size, ok, nil
}

func (f *fakeBlobs) Delete(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.sizes, name)
	f.deleted = append(f.deleted, name)
	return nil
}

func (f *fakeBlobs) ListOlder(_ context.Context, prefix string, t time.Time) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var names []string
	for name, modified := range f.modified {
		if strings.HasPrefix(name, prefix) && modified.Before(t) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names, nil
}

func (f *fakeBlobs) put(name string, size int64, modified time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sizes[name] = size
	f.modified[name] = modified
}

func (f *fakeBlobs) wasDeleted(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Contains(f.deleted, name)
}

var sum = strings.Repeat("0f", 32)

func newService(blobs Blobs, now *time.Time) *Service {
	s := New(blobs, 1000, slog.New(slog.DiscardHandler))
	s.now = func() time.Time { return *now }
	return s
}

func TestCreateValidates(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	s := newService(newFakeBlobs(), &now)
	for _, c := range []struct {
		size int64
		sum  string
		want error
	}{
		{0, sum, ErrInvalidSize},
		{-1, sum, ErrInvalidSize},
		{1001, sum, ErrTooLarge},
		{10, strings.ToUpper(sum), ErrInvalidSHA256},
		{10, sum[:63], ErrInvalidSHA256},
		{10, "", ErrInvalidSHA256},
	} {
		if _, err := s.Create("sb", "u", c.size, c.sum); !errors.Is(err, c.want) {
			t.Errorf("Create(%d, %q) = %v, want %v", c.size, c.sum, err, c.want)
		}
	}
	created, err := s.Create("sb", "u", 1000, sum)
	if err != nil {
		t.Fatal(err)
	}
	if created.Blob != Prefix+created.ID+".tar.gz" || created.PutURL != "put:"+created.Blob ||
		!created.ExpiresAt.Equal(now.Add(PutTTL)) || created.SandboxID != "sb" || created.UserID != "u" {
		t.Fatalf("created = %+v", created)
	}
}

func TestCreateCapsPendingUploadsPerSandbox(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	s := newService(newFakeBlobs(), &now)
	for range MaxPendingPerSandbox {
		if _, err := s.Create("sb", "u", 10, sum); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Create("sb", "u", 10, sum); !errors.Is(err, ErrTooMany) {
		t.Fatalf("over the cap: %v", err)
	}
	if _, err := s.Create("other", "u", 10, sum); err != nil {
		t.Fatalf("another sandbox: %v", err)
	}
	// Expired uploads no longer count.
	now = now.Add(PutTTL)
	if _, err := s.Create("sb", "u", 10, sum); err != nil {
		t.Fatalf("after expiry: %v", err)
	}
}

func TestBeginAndFinish(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	blobs := newFakeBlobs()
	s := newService(blobs, &now)
	ctx := context.Background()
	created, err := s.Create("sb", "u", 100, sum)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.Begin(ctx, created.ID, "sb", "u"); !errors.Is(err, ErrIncomplete) {
		t.Fatalf("before the PUT: %v", err)
	}
	blobs.put(created.Blob, 100, now)
	for _, c := range []struct{ id, sandbox, user string }{
		{"nope", "sb", "u"}, {created.ID, "other", "u"}, {created.ID, "sb", "someone"},
	} {
		if _, err := s.Begin(ctx, c.id, c.sandbox, c.user); !errors.Is(err, ErrNotFound) {
			t.Errorf("Begin(%+v) = %v, want ErrNotFound", c, err)
		}
	}
	blobs.sizeErr = errors.New("storage down")
	if _, err := s.Begin(ctx, created.ID, "sb", "u"); err == nil || errors.Is(err, ErrBusy) {
		t.Fatalf("storage error: %v", err)
	}
	blobs.sizeErr = nil

	ticket, err := s.Begin(ctx, created.ID, "sb", "u")
	if err != nil || ticket.GetURL != "get:"+created.Blob || ticket.SHA256 != sum || ticket.Size != 100 {
		t.Fatalf("ticket = %+v, %v", ticket, err)
	}
	if _, err := s.Begin(ctx, created.ID, "sb", "u"); !errors.Is(err, ErrBusy) {
		t.Fatalf("second Begin: %v", err)
	}
	// Kept for a retry.
	s.Finish(ctx, created.ID, false)
	if blobs.wasDeleted(created.Blob) {
		t.Fatal("a kept upload's blob was deleted")
	}
	if _, err := s.Begin(ctx, created.ID, "sb", "u"); err != nil {
		t.Fatalf("retry: %v", err)
	}
	// Done: gone, blob deleted.
	s.Finish(ctx, created.ID, true)
	if !blobs.wasDeleted(created.Blob) {
		t.Fatal("finished upload's blob kept")
	}
	if _, err := s.Begin(ctx, created.ID, "sb", "u"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after Finish: %v", err)
	}
}

func TestBeginRefusesWrongSizesAndExpiredUploads(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	blobs := newFakeBlobs()
	s := newService(blobs, &now)
	ctx := context.Background()

	wrong, _ := s.Create("sb", "u", 100, sum)
	blobs.put(wrong.Blob, 101, now)
	if _, err := s.Begin(ctx, wrong.ID, "sb", "u"); !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("wrong size: %v", err)
	}
	if !blobs.wasDeleted(wrong.Blob) {
		t.Fatal("a blob of the wrong size was kept")
	}
	if _, err := s.Begin(ctx, wrong.ID, "sb", "u"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after a size mismatch: %v", err)
	}

	late, _ := s.Create("sb", "u", 100, sum)
	blobs.put(late.Blob, 100, now)
	now = now.Add(PutTTL)
	if _, err := s.Begin(ctx, late.ID, "sb", "u"); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired: %v", err)
	}
	if !blobs.wasDeleted(late.Blob) {
		t.Fatal("an expired upload's blob was kept")
	}
}

func TestSweep(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	blobs := newFakeBlobs()
	s := newService(blobs, &now)
	ctx := context.Background()

	expired, _ := s.Create("sb", "u", 10, sum)
	blobs.put(expired.Blob, 10, now)
	busy, _ := s.Create("sb", "u", 10, sum)
	blobs.put(busy.Blob, 10, now)
	if _, err := s.Begin(ctx, busy.ID, "sb", "u"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(PutTTL)
	fresh, _ := s.Create("sb", "u", 10, sum)
	blobs.put(fresh.Blob, 10, now)
	// Left by a restart: no record, and old.
	orphan := Prefix + "orphan.tar.gz"
	blobs.put(orphan, 10, now.Add(-orphanAge-time.Minute))
	recent := Prefix + "recent.tar.gz"
	blobs.put(recent, 10, now.Add(-time.Minute))
	other := "templates/keep"
	blobs.put(other, 10, now.Add(-24*time.Hour))

	s.Sweep(ctx)
	for name, gone := range map[string]bool{
		expired.Blob: true, busy.Blob: false, fresh.Blob: false, orphan: true, recent: false, other: false,
	} {
		if blobs.wasDeleted(name) != gone {
			t.Errorf("%s deleted = %v, want %v", name, !gone, gone)
		}
	}
	// The extraction that outlived the upload finishes, then the sweep
	// takes its blob too.
	s.Finish(ctx, busy.ID, false)
	s.Sweep(ctx)
	if !blobs.wasDeleted(busy.Blob) {
		t.Fatal("expired upload kept after its extraction ended")
	}
}

func TestAzureBlobsSignsOneBlobURLs(t *testing.T) {
	key := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 64)))
	blobs, err := NewAzureBlobs("renderops", key, "spawn-uploads")
	if err != nil {
		t.Fatal(err)
	}
	expiry := time.Now().Add(PutTTL)
	for _, c := range []struct {
		sign  func(string, time.Time) (string, error)
		perms string
	}{{blobs.PutURL, "cw"}, {blobs.GetURL, "r"}} {
		raw, err := c.sign(Prefix+"id-1.tar.gz", expiry)
		if err != nil {
			t.Fatal(err)
		}
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		q := u.Query()
		if u.Scheme != "https" || u.Host != "renderops.blob.core.windows.net" || u.Path != "/spawn-uploads/uploads/id-1.tar.gz" ||
			q.Get("sp") != c.perms || q.Get("sr") != "b" || q.Get("spr") != "https" || q.Get("sig") == "" || q.Get("se") == "" {
			t.Fatalf("signed URL %s", raw)
		}
		if strings.Contains(raw, key) {
			t.Fatal("the account key is in the URL")
		}
	}
	for _, args := range [][3]string{{"", key, "c"}, {"a", "", "c"}, {"a", key, ""}} {
		if _, err := NewAzureBlobs(args[0], args[1], args[2]); err == nil {
			t.Errorf("NewAzureBlobs%v accepted", args)
		}
	}
}
