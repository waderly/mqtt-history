package models

import (
	"time"

	"github.com/spf13/viper"
)

// Bucket ...
type Bucket struct {
	start      time.Time
	bucketSize float64
}

// NewBucket returns an instance of bucket
func NewBucket(config *viper.Viper) (*Bucket, error) {
	layout := "01-02-2006"
	startStr := config.GetString("cassandra.bucket.startDate")
	start, err := time.Parse(layout, startStr)
	if err != nil {
		return nil, err
	}

	bucketSize := time.Duration(config.GetInt("cassandra.bucket.sizeInDays")*24) * time.Hour / time.Second

	return &Bucket{
		start:      start,
		bucketSize: float64(bucketSize),
	}, nil
}

// Get returns the number of buckets (periods of time)
// since start
func (b *Bucket) Get(from int) int {
	println("HENROD",
		time.Unix(int64(from), 0).String(),
		b.start.String(),
		time.Unix(int64(from), 0).Sub(b.start).Hours(),
		b.bucketSize)

	diff := time.Unix(int64(from), 0).Sub(b.start).Hours()
	return int(diff / b.bucketSize)
}
