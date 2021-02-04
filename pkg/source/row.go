package source

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

// Row row
type Row struct {
	Source     string
	DBName     string
	SchemaName string
	TableName  string
	Timestamp  time.Time
	Event      Event
	Items      Items
}

// Items items
type Items []*Item

// Item item
type Item struct {
	Type       FieldType
	Field      string
	Optional   bool
	PrimaryKey bool
	Value      interface{}
}

// IsString string
func (item Item) IsString() bool {
	return item.Type == String
}

// IsInt64 int64
func (item Item) IsInt64() bool {
	return item.Type == Int64
}

// Empty empty
func (r Row) Empty() bool {
	return len(r.Items) == 0
}

// PrimaryKeys
func (r Row) PrimaryKeys() map[string]interface{} {
	rs := make(map[string]interface{}, 0)
	for _, item := range r.Items {
		if item.PrimaryKey {
			rs[item.Field] = item.Value
		}
	}
	return rs
}

// Maps
func (r Row) Maps() map[string]interface{} {
	rs := make(map[string]interface{}, 0)
	for _, item := range r.Items {
		rs[item.Field] = item.Value
	}
	return rs
}

// Params
func (r Row) Params() ([]string, []interface{}) {
	keys := make([]string, 0)
	values := make([]interface{}, 0)
	for _, item := range r.Items {
		keys = append(keys, item.Field)
		values = append(values, item.Value)
	}
	return keys, values
}

// ParseMessage parse consumer message
func ParseMessage(m *sarama.ConsumerMessage) (Row, error) {
	key := &Key{}
	err := json.Unmarshal(m.Key, &key)
	if err != nil {
		return Row{}, errors.Wrap(err, "parse key")
	}

	message := &Message{}
	err = json.Unmarshal(m.Value, &message)
	if err != nil {
		return Row{}, errors.Wrap(err, "parse message")
	}

	row := Row{
		Source:     message.Payload.Source.Connector,
		DBName:     message.Payload.Source.DB,
		SchemaName: message.Payload.Source.Schema,
		TableName:  message.Payload.Source.Table,
		Timestamp:  time.Unix(message.Payload.TsMs/1000, message.Payload.TsMs%1000),
		Event:      message.Payload.OP,
		Items:      make(Items, 0),
	}

	var payload Payload
	var schema string

	switch row.Event {
	case CREATE:
		payload = *message.Payload.After
		schema = "after"
	case UPDATE:
		payload = *message.Payload.After
		schema = "after"
	case DELETE:
		payload = *message.Payload.Before
		schema = "before"
	default:
		return Row{}, fmt.Errorf("event unknow %s", row.Event)
	}

	filed, err := message.Schema.Fields.find(schema)
	if err != nil {
		return Row{}, fmt.Errorf("find schema %s", schema)
	}

	for k, v := range payload {
		fd, err := filed.Fields.find(k)
		if err != nil {
			return Row{}, fmt.Errorf("find field %s", k)
		}
		row.Items = append(row.Items, &Item{
			Type:       fd.Type,
			Field:      k,
			Value:      v,
			Optional:   fd.Optional,
			PrimaryKey: key.isPrimary(k),
		})
	}

	return row, nil
}
