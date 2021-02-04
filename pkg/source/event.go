package source

// Event event
type Event string

// event
const (
	CREATE Event = "c" // insert
	UPDATE Event = "u" // update
	DELETE Event = "d" // delete
)

// EventNames 名称
var EventNames = map[Event]string{
	CREATE: "create",
	UPDATE: "update",
	DELETE: "delete",
}

// Name 获取名称
func (e Event) Name() string {
	if v, ok := EventNames[e]; ok {
		return v
	}
	return string(e)
}
