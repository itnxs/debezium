package connect

import (
	"context"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/Masterminds/squirrel"
	"github.com/itnxs/debezium/pkg/config"
	"github.com/itnxs/debezium/pkg/source"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// ClickHouseConnect ClickHouse连接器
type ClickHouseConnect struct {
	db *sqlx.DB
}

// NewClickHouseConnect 新建ClickHouse连接器
func NewClickHouseConnect() (*ClickHouseConnect, error) {
	db, err := sqlx.Open("clickhouse", config.GetConfig().ClickHouse.DNS)
	if err != nil {
		return nil, errors.Wrap(err, "open clickhouse")
	}
	if err := db.Ping(); err != nil {
		return nil, errors.Wrap(err, "ping clickhouse")
	}
	return &ClickHouseConnect{db: db}, nil
}

func (c *ClickHouseConnect) tableName(row source.Row) string {
	return config.GetConfig().ClickHouse.TableName(row.TableName)
}

// Create 创建数据
func (c *ClickHouseConnect) Create(ctx context.Context, row source.Row) error {
	return nil
}

// Update 修改数据
func (c *ClickHouseConnect) Update(ctx context.Context, row source.Row) error {
	return nil
}

// Delete 删除数据
func (c *ClickHouseConnect) Delete(ctx context.Context, row source.Row) error {
	return nil
}
