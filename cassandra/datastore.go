package cassandra

import (
	"context"
	"fmt"
	"time"

	"github.com/topfreegames/mqtt-history/models"
)

// DataStore is the interface with data access methods
type DataStore interface {
	SelectMessagesInBucket(
		ctx context.Context, topic string, from, limit int,
	) []*models.Message
}

// SelectMessagesInBucket gets at most limit messages on
// topic and bucket from Cassandra.
func (s *Store) SelectMessagesInBucket(
	ctx context.Context,
	topic string,
	from, limit int,
) []*models.Message {
	query := fmt.Sprintf(`
	SELECT payload, toTimestamp(id) as timestamp, topic
	FROM messages 
	WHERE topic = ? AND bucket = ?
	LIMIT %d
	`, limit)

	messages := []*models.Message{}
	bucket := s.bucket.Get(from)

	fmt.Printf("HENROD %s %s %d", query, topic, bucket)

	iter := s.DBSession.Query(query, topic, bucket).WithContext(ctx).Iter()
	defer iter.Close()
	for {
		var payload, topic string
		var timestamp time.Time
		if !iter.Scan(&payload, &timestamp, &topic) {
			break
		}
		messages = append(messages, &models.Message{
			Timestamp: timestamp,
			Payload:   payload,
			Topic:     topic,
		})
	}

	return messages
}
