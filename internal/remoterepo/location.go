package remoterepo

import (
	"context"
	"strings"

	"go.kenn.io/msgvault/internal/objstore"
)

// Location names an offload repository: a filesystem path (external drive,
// NAS mount, rclone mount) or an s3://bucket/prefix URL with optional
// endpoint/region for S3-compatible providers.
type Location struct {
	Repo       string
	S3Endpoint string
	S3Region   string
}

// OpenLocation opens the appropriate backend for loc.
func OpenLocation(ctx context.Context, loc Location) (Repo, error) {
	if strings.HasPrefix(loc.Repo, "s3://") {
		bucket, prefix, err := objstore.ParseS3URL(loc.Repo)
		if err != nil {
			return nil, err
		}
		store, err := objstore.NewS3Store(objstore.S3Config{
			Bucket: bucket, Prefix: prefix,
			Region: loc.S3Region, Endpoint: loc.S3Endpoint,
		}, nil)
		if err != nil {
			return nil, err
		}
		return OpenObjectStore(ctx, store)
	}
	return Open(loc.Repo)
}
