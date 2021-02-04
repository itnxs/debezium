package source

import "fmt"

// FieldType type
type FieldType string

// field type
const (
	Struct FieldType = "struct"
	String FieldType = "string"
	Int64  FieldType = "int64"
)

// Fields fields
type Fields []Field

// Schema schema
type Schema struct {
	Type     FieldType
	Optional bool
	Fields   Fields
	Name     string
}

// Field field
type Field struct {
	Type     FieldType `json:"type"`
	Optional bool      `json:"optional"`
	Field    string    `json:"field"`
	Name     string    `json:"name"`
	Fields   Fields    `json:"fields"`
}

// Key key
type Key struct {
	Schema  Schema  `json:"schema"`
	Payload Payload `json:"payload"`
}

// Message message
type Message struct {
	Schema  Schema `json:"schema"`
	Payload struct {
		Before      *Payload     `json:"before"`
		After       *Payload     `json:"after"`
		Source      Source       `json:"source"`
		OP          Event        `json:"op"`
		TsMs        int64        `json:"ts_ms"`
		Transaction *Transaction `json:"transaction"`
	} `json:"payload"`
}

// Payload payload
type Payload map[string]interface{}

// Source source
type Source struct {
	Version   string  `json:"version"`
	Connector string  `json:"connector"`
	Name      string  `json:"name"`
	Snapshot  string  `json:"snapshot"`
	File      string  `json:"file"`
	DB        string  `json:"db"`
	Schema    string  `json:"Schema"`
	Table     string  `json:"table"`
	TsMs      int64   `json:"ts_ms"`
	ServerID  int64   `json:"server_id"`
	Pos       int64   `json:"pos"`
	Row       int64   `json:"row"`
	Thread    int64   `json:"thread"`
	GtID      *string `json:"gtid"`
	Query     *string `json:"query"`
}

// Transaction transaction
type Transaction struct {
	ID                  string `json:"id"`
	TotalOrder          int64  `json:"total_order"`
	DataCollectionOrder int64  `json:"data_collection_order"`
}

// find find field
func (fs Fields) find(name string) (Field, error) {
	for _, v := range fs {
		if v.Field == name {
			return v, nil
		}
	}
	return Field{}, fmt.Errorf("filed %s unknow", name)
}

// isPrimary primary key
func (k Key) isPrimary(name string) bool {
	_, ok := k.Payload[name]
	return ok
}


