package debezium

import (
	"context"
	"fmt"
	"path"
	"reflect"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/itnxs/debezium/pkg/config"
	"github.com/itnxs/debezium/pkg/connect"
	"github.com/itnxs/debezium/pkg/kafka"
	"github.com/itnxs/debezium/pkg/source"
	"github.com/sirupsen/logrus"
)

// Server 服务
type Server struct {
	client   *kafka.Client
	connects connect.Connects
}

// NewServer 新建服务
func NewServer() (*Server, error) {
	c := config.GetConfig()
	client, err := kafka.NewClient(c.Kafka.Brokers)
	if err != nil {
		return nil, err
	}

	connects := make(connect.Connects, 0)
	if c.Mysql.Enable {
		mc, err := connect.NewMysqlConnect()
		if err != nil {
			return nil, err
		}
		connects = append(connects, mc)
		logrus.Info("add MysqlConnect")
	}

	if c.Pgsql.Enable {
		pg, err := connect.NewPgsqlConnect()
		if err != nil {
			return nil, err
		}
		connects = append(connects, pg)
		logrus.Info("add PgsqlConnect")
	}

	if c.ES.Enable {
		es, err := connect.NewElasticConnect()
		if err != nil {
			return nil, err
		}
		connects = append(connects, es)
		logrus.Info("add ElasticConnect")
	}

	if c.ClickHouse.Enable {
		ch, err := connect.NewClickHouseConnect()
		if err != nil {
			return nil, err
		}
		connects = append(connects, ch)
		logrus.Info("add ClickHouseConnect")
	}

	return &Server{
		client:   client,
		connects: connects,
	}, nil
}

// Run 运行
func (s *Server) Run(ctx context.Context) error {
	logrus.Info("debezium server start")
	c := config.GetConfig()
	defer s.client.Close()
	return s.client.Start(ctx, c.Kafka.Topic, c.Kafka.Group, s.handler)
}

// handler 处理
func (s *Server) handler(ctx context.Context, session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) error {
	defer session.MarkMessage(message, "")

	row, err := source.ParseMessage(message)
	if err != nil {
		return err
	}

	if row.Empty() {
		return nil
	}

	for _, c := range s.connects {
		err := s.change(ctx, c, row)
		if err != nil {
			continue
		}
	}

	return nil
}

// change 数据变更
func (s *Server) change(ctx context.Context, c connect.Connect, row source.Row) (err error) {
	st := time.Now()
	name := strings.Trim(path.Ext(fmt.Sprintf("%s", reflect.TypeOf(c))), ".")

	defer func() {
		l := logrus.WithField("event", row.Event.Name()).WithField("runtime", time.Now().Sub(st).Milliseconds())
		if err != nil {
			l.WithError(err).Error(name)
		} else {
			l.Info(name)
		}
	}()

	switch row.Event {
	case source.CREATE:
		err = c.Create(ctx, row)
	case source.UPDATE:
		err = c.Update(ctx, row)
	case source.DELETE:
		err = c.Delete(ctx, row)
	}

	return err
}
