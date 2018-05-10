package cassandra

import (
	"context"
	"fmt"
	"time"

	cassdatastore "git.topfreegames.com/topfreegames/adspot/cassandra"
	"github.com/gocql/gocql"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/topfreegames/extensions/cassandra"
	"github.com/topfreegames/extensions/middleware"
)

func sendMetrics(ctx context.Context, mr middleware.MetricsReporter, keyspace string, elapsed time.Duration, logger logrus.FieldLogger) {
	logger = logger.WithField("operation", "cassandra.sendMetrics")

	logger.Debug("sending metrics do statsd")

	if mr == nil || ctx == nil {
		if mr == nil {
			logger.Debug("MetricsReporter is nil")
		} else {
			logger.Debug("ctx is nil")
		}
		return
	}

	tags := []string{fmt.Sprintf("keyspace:%s", keyspace)}

	if val, ok := ctx.Value("queryName").(string); ok {
		tags = append(tags, fmt.Sprintf("queryName:%s", val))
	}

	logger.Debug("sending metrics to statsd")

	if err := mr.Timing("cassandraQuery", elapsed, tags...); err != nil {
		logger.WithError(err).Error("failed to send metric to statsd")
	}
}

// QueryObserver implements gocql.QueryObserver
type QueryObserver struct {
	logger          logrus.FieldLogger
	MetricsReporter middleware.MetricsReporter
}

// ObserveQuery sends timing metrics to dogstatsd on every query
func (o *QueryObserver) ObserveQuery(ctx context.Context, q gocql.ObservedQuery) {
	sendMetrics(o.MetricsReporter, ctx, q.Keyspace, q.End.Sub(q.Start), o.logger)
}

// BatchObserver implements gocql.BatchObserver
type BatchObserver struct {
	logger          logrus.FieldLogger
	MetricsReporter middleware.MetricsReporter
}

// ObserveBatch sends timing metrics to dogstatsd on every batch query
func (o *BatchObserver) ObserveBatch(ctx context.Context, b gocql.ObservedBatch) {
	sendMetrics(o.MetricsReporter, ctx, b.Keyspace, b.End.Sub(b.Start), o.logger)
}

// GetCassandra connects on Cassandra and returns the client with a session
func GetCassandra(
	logger logrus.FieldLogger,
	config *viper.Viper,
	mr middleware.MetricsReporter,
) (cassdatastore.Datastore, error) {
	l := logger.WithField("operation", "cassandra.GetCassandra")

	params := &cassandra.ClientParams{
		ClusterConfig: cassandra.ClusterConfig{
			Prefix:        "cassandra",
			QueryObserver: &QueryObserver{logger: logger, MetricsReporter: mr},
			BatchObserver: &BatchObserver{logger: logger, MetricsReporter: mr},
		},
		Config: config,
	}

	client, err := cassandra.NewClient(params)
	if err != nil {
		l.WithError(err).Error("connection to database failed")
		return nil, err
	}

	l.Info("successfully connected to cassandra")
	store := &cassdatastore.Store{DBSession: client.Session}

	return store, nil
}
