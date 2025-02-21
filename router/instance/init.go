package instance

import (
	"context"
)

type RouterMetadataBootstrapper interface {
	InitializeMetadata(ctx context.Context, r RouterInstance) error
}
