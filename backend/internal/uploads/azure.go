package uploads

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
)

// AzureBlobs keeps uploads in one Azure Blob container. It signs service SAS
// URLs with the storage account's shared key: each URL is for one blob,
// HTTPS only, and either create/write (the client's PUT) or read (the
// sandbox's download).
type AzureBlobs struct {
	cred      *azblob.SharedKeyCredential
	container *container.Client
	name      string
}

var _ Blobs = (*AzureBlobs)(nil)

// NewAzureBlobs uses container containerName of storage account account.
func NewAzureBlobs(account, accountKey, containerName string) (*AzureBlobs, error) {
	if account == "" || accountKey == "" || containerName == "" {
		return nil, fmt.Errorf("upload storage requires an account, a key and a container")
	}
	cred, err := azblob.NewSharedKeyCredential(account, accountKey)
	if err != nil {
		return nil, fmt.Errorf("azure shared-key credential: %w", err)
	}
	containerURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s", account, containerName)
	client, err := container.NewClientWithSharedKeyCredential(containerURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("azure container client: %w", err)
	}
	return &AzureBlobs{cred: cred, container: client, name: containerName}, nil
}

func (a *AzureBlobs) PutURL(name string, expiry time.Time) (string, error) {
	return a.sign(name, sas.BlobPermissions{Create: true, Write: true}, expiry)
}

func (a *AzureBlobs) GetURL(name string, expiry time.Time) (string, error) {
	return a.sign(name, sas.BlobPermissions{Read: true}, expiry)
}

func (a *AzureBlobs) sign(name string, permissions sas.BlobPermissions, expiry time.Time) (string, error) {
	query, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     time.Now().UTC().Add(-clockSkew),
		ExpiryTime:    expiry.UTC(),
		Permissions:   permissions.String(),
		ContainerName: a.name,
		BlobName:      name,
	}.SignWithSharedKey(a.cred)
	if err != nil {
		return "", fmt.Errorf("sign SAS for %s: %w", name, err)
	}
	return a.container.NewBlobClient(name).URL() + "?" + query.Encode(), nil
}

func (a *AzureBlobs) Size(ctx context.Context, name string) (int64, bool, error) {
	props, err := a.container.NewBlobClient(name).GetProperties(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("stat %s: %w", name, err)
	}
	if props.ContentLength == nil {
		return 0, true, nil
	}
	return *props.ContentLength, true, nil
}

func (a *AzureBlobs) Delete(ctx context.Context, name string) error {
	if _, err := a.container.NewBlobClient(name).Delete(ctx, nil); err != nil && !bloberror.HasCode(err, bloberror.BlobNotFound) {
		return fmt.Errorf("delete %s: %w", name, err)
	}
	return nil
}

func (a *AzureBlobs) ListOlder(ctx context.Context, prefix string, t time.Time) ([]string, error) {
	var names []string
	pager := a.container.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &prefix})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", prefix, err)
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil || item.Properties == nil || item.Properties.LastModified == nil {
				continue
			}
			if item.Properties.LastModified.Before(t) {
				names = append(names, *item.Name)
			}
		}
	}
	return names, nil
}
