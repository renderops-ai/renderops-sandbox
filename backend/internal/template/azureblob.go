package template

import (
	"context"
	"fmt"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
)

// AzureBlobStore is an ArtifactStore backed by an Azure Blob container. It is the
// durable store the builder publishes to (and, later, the read-only source workers
// sync from). Credentials come from the environment — never hardcoded.
//
// NOTE: this uses a shared account KEY (full read/write). That is appropriate for
// the BUILDER (it needs write). Runtime WORKERS should use a read-only SAS instead
// of the account key (same invariant that keeps DB creds off untrusted workers).
type AzureBlobStore struct {
	client    *azblob.Client
	container string
}

// NewAzureBlobStore builds a store for account/container using a shared account key.
func NewAzureBlobStore(account, accountKey, container string) (*AzureBlobStore, error) {
	if account == "" || accountKey == "" || container == "" {
		return nil, fmt.Errorf("azure blob store requires account, key and container")
	}
	cred, err := azblob.NewSharedKeyCredential(account, accountKey)
	if err != nil {
		return nil, fmt.Errorf("azure shared-key credential: %w", err)
	}
	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", account)
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("azure blob client: %w", err)
	}
	return &AzureBlobStore{client: client, container: container}, nil
}

var _ ArtifactStore = (*AzureBlobStore)(nil)

func (s *AzureBlobStore) Put(ctx context.Context, key string, r io.Reader) error {
	if _, err := s.client.UploadStream(ctx, s.container, key, r, nil); err != nil {
		return fmt.Errorf("upload %s: %w", key, err)
	}
	return nil
}

// Delete removes key. A key that does not exist is not an error.
func (s *AzureBlobStore) Delete(ctx context.Context, key string) error {
	if _, err := s.client.DeleteBlob(ctx, s.container, key, nil); err != nil && !bloberror.HasCode(err, bloberror.BlobNotFound) {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

func (s *AzureBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := s.client.DownloadStream(ctx, s.container, key, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, fmt.Errorf("%s: %w", key, ErrNotFound)
		}
		return nil, fmt.Errorf("download %s: %w", key, err)
	}
	return resp.Body, nil
}

func (s *AzureBlobStore) Stat(ctx context.Context, key string) (int64, bool, error) {
	blobClient := s.client.ServiceClient().NewContainerClient(s.container).NewBlobClient(key)
	props, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("stat %s: %w", key, err)
	}
	var size int64
	if props.ContentLength != nil {
		size = *props.ContentLength
	}
	return size, true, nil
}

func (s *AzureBlobStore) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	pager := s.client.NewListBlobsFlatPager(s.container, &azblob.ListBlobsFlatOptions{Prefix: &prefix})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", prefix, err)
		}
		for _, b := range page.Segment.BlobItems {
			if b.Name != nil {
				keys = append(keys, *b.Name)
			}
		}
	}
	return keys, nil
}
