package connect

import (
	"context"
	"github.com/itnxs/debezium/pkg/config"
	"github.com/itnxs/debezium/pkg/source"

	"github.com/Masterminds/squirrel"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// MysqlConnect MYSQL连接器
type MysqlConnect struct {
	db *sqlx.DB
}

// NewMysqlConnect 新建MYSQL连接器
func NewMysqlConnect() (*MysqlConnect, error) {
	db, err := sqlx.Open("mysql", config.GetConfig().Mysql.DNS)
	if err != nil {
		return nil, errors.Wrap(err, "open mysql")
	}
	return &MysqlConnect{db: db}, nil
}

func (c *MysqlConnect) tableName(row source.Row) string {
	return config.GetConfig().Mysql.TableName(row.TableName)
}

// Create 创建数据
func (c *MysqlConnect) Create(ctx context.Context, row source.Row) error {
	columns, values := row.Params()
	query, args, err := squirrel.
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
func (c *MysqlConnect) Update(ctx context.Context, row source.Row) error {
	query, args, err := squirrel.
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
func (c *MysqlConnect) Delete(ctx context.Context, row source.Row) error {
	query, args, err := squirrel.
		Delete(c.tableName(row)).
		Where(row.PrimaryKeys()).
		ToSql()
	if err == nil {
		_, err = c.db.ExecContext(ctx, query, args...)
	}
	return errors.Wrapf(err, "query: %s, args: %v", query, args)
}
