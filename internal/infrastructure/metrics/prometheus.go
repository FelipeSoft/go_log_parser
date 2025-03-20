package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	LogProcessingBySeconds = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "log_processing_per_seconds",
			Help: "Processed Logs per Seconds",
		},
	)

	LogProcessingLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "log_processing_latency_milliseconds",
			Help:    "Latency of log processing in milliseconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"service"},
	)
)

func init() {
	prometheus.Register(LogProcessingBySeconds)
	prometheus.Register(LogProcessingLatency)
}
