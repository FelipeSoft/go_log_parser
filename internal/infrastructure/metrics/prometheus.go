package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	ReceivedMessages = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "received_messages_total",
		Help: "Total number of received messages",
	})

	ProcessedMessages = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "processed_messages_total",
		Help: "Total number of successfully processed messages",
	})

	DroppedMessages = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dropped_messages_total",
		Help: "Total number of dropped messages due to backpressure",
	})

	LogsPerSecond = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:    "logs_per_second_total",
			Help:    "Log Processed per Seconds",
		},
		[]string{"log_per_second_service"},
	)

	LogProcessingLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "log_latency_milliseconds",
			Help:    "Latency of log processing in milliseconds",
			Buckets: []float64{50, 100, 250, 500, 1000, 2000, 5000},
		},
		[]string{"log_latency_milliseconds_service"},
	)
)

func init() {
	prometheus.MustRegister(LogsPerSecond)
	prometheus.MustRegister(LogProcessingLatency)
	prometheus.MustRegister(ReceivedMessages)
	prometheus.MustRegister(ProcessedMessages)
	prometheus.MustRegister(DroppedMessages)
}
