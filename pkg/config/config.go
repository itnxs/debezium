package config

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
	"github.com/pkg/errors"
)

var c = Config{}

// Config config
type Config struct {
	Kafka      Kafka      `toml:"kafka"`
	Mysql      Mysql      `toml:"mysql"`
	Pgsql      Pgsql      `toml:"pgsql"`
	ClickHouse ClickHouse `toml:"clickhouse"`
	ES         ES         `toml:"elasticsearch"`
}

// Kafka kafka
type Kafka struct {
	Brokers string `toml:"brokers"`
	Topic   string `toml:"topic"`
	Group   string `toml:"group"`
}

// Mysql mysql
type Mysql struct {
	Enable bool              `toml:"enable"`
	DNS    string            `toml:"dns"`
	Tables map[string]string `toml:"tables"`
}

// Pgsql pgsql
type Pgsql struct {
	Enable bool              `toml:"enable"`
	DNS    string            `toml:"dns"`
	Tables map[string]string `toml:"tables"`
}

// ClickHouse clickhouse
type ClickHouse struct {
	Enable bool              `toml:"enable"`
	DNS    string            `toml:"dns"`
	Tables map[string]string `toml:"tables"`
}

// ES es
type ES struct {
	Enable   bool              `toml:"enable"`
	URL      string            `toml:"url"`
	User     string            `toml:"user"`
	Password string            `toml:"password"`
	Indexes  map[string]string `toml:"indexes"`
	Type     string            `toml:"type"`
}

// Load load
func Load(file string) error {
	data, err := ioutil.ReadFile(file)
	if err == nil {
		err = toml.Unmarshal(data, &c)
	}
	return errors.WithStack(err)
}

// GetConfig get config
func GetConfig() Config {
	return c
}

// TableName mysql table name
func (m Mysql) TableName(name string) string {
	if v, ok := m.Tables[name]; ok {
		return v
	}
	return name
}

// TableName pgsql table name
func (m Pgsql) TableName(name string) string {
	if v, ok := m.Tables[name]; ok {
		return v
	}
	return name
}

// TableName clickhouse table name
func (m ClickHouse) TableName(name string) string {
	if v, ok := m.Tables[name]; ok {
		return v
	}
	return name
}

// IndexName es index name
func (m ES) IndexName(name string) string {
	if v, ok := m.Indexes[name]; ok {
		return v
	}
	return name
}
