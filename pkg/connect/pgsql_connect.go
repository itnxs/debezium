package connect

import (
	"context"

	"github.com/Masterminds/squirrel"
	"github.com/itnxs/debezium/pkg/config"
	"github.com/itnxs/debezium/pkg/source"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// PgsqlConnect PGSQL连接器
type PgsqlConnect struct {
	db *sqlx.DB
}

// NewPgsqlConnect 新建PGSQL连接器
func NewPgsqlConnect() (*PgsqlConnect, error) {
	db, err := sqlx.Open("pgx", config.GetConfig().Pgsql.DNS)
	if err != nil {
		return nil, errors.Wrap(err, "open pgsql")
	}
	if err := db.Ping(); err != nil {
		return nil, errors.Wrap(err, "ping pgsql")
	}
	return &PgsqlConnect{db: db}, nil
}

func (c *PgsqlConnect) tableName(row source.Row) string {
	return config.GetConfig().Pgsql.TableName(row.TableName)
}

// Create 创建数据
func (c *PgsqlConnect) Create(ctx context.Context, row source.Row) error {
	columns, values := row.Params()
	query, args, err := squirrel.
		StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Insert(c.tableName(row)).
		Columns(columns...).
		Values(values...).
		ToSql()
	if err == nil {
		_, err = c.db.ExecContext(ctx, query, args...)
	}
	return errors.Wrapf(err, "query: %s, args: %v", query, args)
}

// Update 修改数据
func (c *PgsqlConnect) Update(ctx context.Context, row source.Row) error {
	query, args, err := squirrel.
		StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Update(c.tableName(row)).
		SetMap(row.Maps()).
		Where(row.PrimaryKeys()).
		ToSql()
	if err == nil {
		_, err = c.db.ExecContext(ctx, query, args...)
	}
	return errors.Wrapf(err, "query: %s, args: %v", query, args)
}

// Delete 删除数据
func (c *PgsqlConnect) Delete(ctx context.Context, row source.Row) error {
	query, args, err := squirrel.
		StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Delete(c.tableName(row)).
		Where(row.PrimaryKeys()).
		ToSql()
	if err == nil {
		_, err = c.db.ExecContext(ctx, query, args...)
	}
	return errors.Wrapf(err, "query: %s, args: %v", query, args)
}
