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
	diff := time.Unix(int64(from), 0).Sub(b.start).Seconds()

	buckets := int(diff / b.bucketSize)
	if buckets < 0 {
		buckets = 0
	}

	return buckets
}

// Range returns a list of buckets starting in from and ending in since
func (b *Bucket) Range(from, to int) []int {
	bucketFrom := b.Get(from)
	bucketTo := b.Get(to)

	buckets := make([]int, bucketFrom-bucketTo+1)
	idx := 0
	for i := bucketTo; i <= bucketFrom; i++ {
		buckets[idx] = i
		idx = idx + 1
	}

	return buckets
}
