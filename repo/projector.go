package repo

import "github.com/quintans/eventstore"

type Projector interface {
	Project(eventstore.Event)
}
