package actor

type Event struct {
	Name string
}

var (
	EventStart = *Event{"start"}
	EventStop  = *Event{"stop"}
)
