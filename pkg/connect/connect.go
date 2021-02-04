package connect

import (
	"context"

	"github.com/itnxs/debezium/pkg/source"
)

// Connects 链接器集合
type Connects []Connect

// Connect 连接器接口
type Connect interface {
	Create(ctx context.Context, row source.Row) error
	Update(ctx context.Context, row source.Row) error
	Delete(ctx context.Context, row source.Row) error
}
