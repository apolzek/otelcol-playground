package sender

import (
	"context"

	"google.golang.org/grpc/metadata"
)

func withMetadata(ctx context.Context, hdr map[string]string) context.Context {
	md := make(metadata.MD, len(hdr))
	for k, v := range hdr {
		md.Set(k, v)
	}
	return metadata.NewOutgoingContext(ctx, md)
}
