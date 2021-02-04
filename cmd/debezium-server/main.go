package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/itnxs/debezium/pkg/config"
	"github.com/itnxs/debezium/pkg/debezium"
	"github.com/sirupsen/logrus"
)

var configFile string

func init() {
	flag.StringVar(&configFile, "conf", "./etc/config.toml", "load config file")
	flag.Parse()

	err := config.Load(configFile)
	if err != nil {
		logrus.Panicln(err)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	go watch(cancel)

	s, err := debezium.NewServer()
	if err != nil {
		logrus.WithError(err).Panic("new debezium server")
	}

	err = s.Run(ctx)
	if err != nil {
		logrus.WithError(err).Panic("debezium server start")
	}
}

func watch(cancel context.CancelFunc) {
	sign := make(chan os.Signal, 1)
	signal.Notify(sign, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGSTOP)
	s := <-sign
	logrus.WithField("signal", s.String()).Info("debezium server stop")
	cancel()
}
