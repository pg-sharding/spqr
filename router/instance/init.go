package instance

import (
	"context"
)

type RouterMetadataBootstraper interface {
	InitializeMetadata(ctx context.Context, r RouterInstance) error
}
