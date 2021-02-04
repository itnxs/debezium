package connect

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/itnxs/debezium/pkg/config"
	"github.com/itnxs/debezium/pkg/source"
	"github.com/olivere/elastic"
	"github.com/pkg/errors"
)

// ElasticConnect ES连接器
type ElasticConnect struct {
	es *elastic.Client
}

// NewElasticConnect 新建ES连接器
func NewElasticConnect() (*ElasticConnect, error) {
	es, err := elastic.NewClient(
		elastic.SetSniff(false),
		elastic.SetURL(strings.Split(config.GetConfig().ES.URL, ",")...),
		elastic.SetBasicAuth(config.GetConfig().ES.User, config.GetConfig().ES.Password),
	)
	if err != nil {
		return nil, errors.Wrap(err, "new elastic client")
	}
	return &ElasticConnect{es: es}, nil
}

func (c *ElasticConnect) indexName(row source.Row) string {
	return config.GetConfig().ES.IndexName(row.TableName)
}

func (c *ElasticConnect) typeName() string {
	if config.GetConfig().ES.Type == "" {
		return "_doc"
	}
	return config.GetConfig().ES.Type
}

func (c *ElasticConnect) idName(row source.Row) string {
	keys := row.PrimaryKeys()

	ks := make([]string, 0)
	for k := range row.PrimaryKeys() {
		ks = append(ks, k)
	}

	sort.Strings(ks)

	ids := make([]string, 0)
	for _, k := range ks {
		ids = append(ids, fmt.Sprintf("%v", keys[k]))
	}

	return strings.Join(ids, "_")
}

// Create 创建
func (c *ElasticConnect) Create(ctx context.Context, row source.Row) error {
	_, err := c.es.Index().
		Index(c.indexName(row)).
		Type(c.typeName()).
		Id(c.idName(row)).
		BodyJson(row.Maps()).
		Do(ctx)
	return errors.WithStack(err)
}

// Update 更新
func (c *ElasticConnect) Update(ctx context.Context, row source.Row) error {
	_, err := c.es.Index().
		Index(c.indexName(row)).
		Type(c.typeName()).
		Id(c.indexName(row)).
		BodyJson(row.Maps()).
		Do(ctx)
	return errors.WithStack(err)
}

// Delete 删除
func (c *ElasticConnect) Delete(ctx context.Context, row source.Row) error {
	_, err := c.es.Delete().Index(c.indexName(row)).Id(c.idName(row)).Do(ctx)
	return errors.WithStack(err)
}
