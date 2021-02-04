package kafka

import (
	"context"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Handler 处理程序
type Handler func(context.Context, sarama.ConsumerGroupSession, *sarama.ConsumerMessage) error

// Client 客户端
type Client struct {
	c sarama.Client
}

// NewClient 新建客户端
func NewClient(addr string) (*Client, error) {
	c := sarama.NewConfig()
	c.Version = sarama.V2_1_0_0

	if err := c.Validate(); err != nil {
		return nil, errors.Wrap(err, "validate kafka config")
	}

	client, err := sarama.NewClient(strings.Split(addr, ","), c)
	if err != nil {
		return nil, errors.Wrap(err, "new kafka client")
	}

	return &Client{c: client}, nil
}

// Start 启动
func (c *Client) Start(ctx context.Context, topic, group string, handler Handler) error {
	consumer, err := sarama.NewConsumerGroupFromClient(group, c.c)
	if err != nil {
		return errors.Wrap(err, "new consumer client")
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := consumer.Consume(ctx, strings.Split(topic, ","), &process{ctx: ctx, handler: handler})
				if err == nil {
					continue
				}
				logrus.WithError(err).WithField("topic", topic).Error("client consume error")
				time.Sleep(time.Second)
			}
		}
	}()

	<-ctx.Done()
	err = consumer.Close()
	return errors.Wrap(err, "consumer close")
}

// Close 关闭客户端
func (c *Client) Close() error {
	err := c.c.Close()
	return errors.Wrap(err, "client close")
}

type process struct {
	ctx context.Context
	handler Handler
}

func (h *process) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *process) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *process) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		if err := h.handler(h.ctx, session, message); err != nil {
			logrus.WithError(err).Error("error")
			return err
		}
	}
	return nil
}
