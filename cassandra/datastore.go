package cassandra

import (
	cassandra "github.com/topfreegames/extensions/cassandra/interfaces"
)

// Store is the access layer and contains the cassandra session
type Store struct {
	DBSession cassandra.Session
}
