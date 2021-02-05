package connect

import (
	"context"
	"fmt"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/Masterminds/squirrel"
	"github.com/itnxs/debezium/pkg/config"
	"github.com/itnxs/debezium/pkg/source"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// ClickHouseConnect ClickHouse连接器
type ClickHouseConnect struct {
	db       *sqlx.DB
	isCreate bool
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

func (c *ClickHouseConnect) createTable(row source.Row) error {
	if c.isCreate {
		return nil
	}
	c.isCreate = true

	keys := make([]string, 0)
	fields := make([]string, 0)
	for _, item := range row.Items {
		fields = append(fields, fmt.Sprintf("%s %s%s", item.Field, strings.ToUpper(string(item.Type[:1])), item.Type[1:]))
		if item.PrimaryKey {
			keys = append(keys, item.Field)
		}
	}

	var pk string
	if len(keys) > 1 {
		pk = fmt.Sprintf("(%s)", strings.Join(keys, ","))
	} else {
		pk = keys[0]
	}

	createSQL := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s(%s) ENGINE = MergeTree PARTITION BY %s ORDER BY %s PRIMARY KEY %s;`,
		c.tableName(row), strings.Join(fields, ","), pk, pk, pk)

	_, err := c.db.Exec(createSQL)
	return errors.Wrapf(err, "create table: %s", createSQL)
}

func (c *ClickHouseConnect) exec(ctx context.Context, query string, args ...interface{}) error {
	tx, err := c.db.Beginx()
	if err != nil {
		return errors.Wrap(err, "begin")
	}

	smt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return errors.Wrapf(err, "prepare: %s", query)
	}

	defer smt.Close()

	_, err = smt.ExecContext(ctx, args...)
	if err != nil {
		return errors.Wrapf(err, "query: %s, smt exec: %v", query, args)
	}

	err = tx.Commit()
	return errors.Wrap(err, "commit")
}

// Create 创建数据
func (c *ClickHouseConnect) Create(ctx context.Context, row source.Row) error {
	if err := c.createTable(row); err != nil {
		return err
	}

	columns, values := row.Params()
	query, args, err := squirrel.
		Insert(c.tableName(row)).
		Columns(columns...).
		Values(values...).
		ToSql()
	if err != nil {
		return errors.WithStack(err)
	}

	return c.exec(ctx, query, args...)
}

// Update 修改数据
func (c *ClickHouseConnect) Update(ctx context.Context, row source.Row) error {
	query, args, err := squirrel.
		Update(c.tableName(row)).
		SetMap(row.Updates()).
		Where(row.PrimaryKeys()).
		ToSql()
	if err != nil {
		return errors.WithStack(err)
	}
	query = strings.Replace(query, "UPDATE ", "ALTER TABLE ", 1)
	query = strings.Replace(query, "SET ", "UPDATE ", 1)
	return c.exec(ctx, query, args...)
}

// Delete 删除数据
func (c *ClickHouseConnect) Delete(ctx context.Context, row source.Row) error {
	query, args, err := squirrel.
		Delete(c.tableName(row)).
		Where(row.PrimaryKeys()).
		ToSql()
	if err != nil {
		return errors.WithStack(err)
	}
	query = strings.Replace(query, "DELETE FROM ", "ALTER TABLE ", 1)
	query = strings.Replace(query, " WHERE", " DELETE WHERE", 1)
	return c.exec(ctx, query, args...)
}
