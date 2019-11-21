package othttp

import (
	"net/http"
	"time"

	"go.opentelemetry.io/otel/api/key"
	"go.opentelemetry.io/otel/api/metric"
)

var (
	pathKey   = key.New("http.path")
	statusKey = key.New("http.status")
)

// NewHandler handles it
func NewHandler(h http.Handler, m metric.Meter) http.Handler {
	mw := &mw{
		h:       h,
		latency: m.NewInt64Measure("ot_server_latency"),
		count:   m.NewInt64Counter("ot_request_count", metric.WithKeys(pathKey), metric.WithKeys(statusKey)),
		m:       m,
	}
	return mw
}

type mw struct {
	h       http.Handler
	latency metric.Int64Measure
	count   metric.Int64Counter
	m       metric.Meter
}

type responseWrapper struct {
	http.ResponseWriter
	code int
}

func (rw *responseWrapper) WriteHeader(statusCode int) {
	rw.code = statusCode
	rw.ResponseWriter.WriteHeader(statusCode)
}

func (rw *responseWrapper) Code() int {
	if rw.code == 0 {
		rw.code = 200
	}
	return rw.code
}

func (mw *mw) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t := time.Now()
	rw := &responseWrapper{w, 0}
	mw.h.ServeHTTP(rw, r)
	requestDuration := time.Since(t).Milliseconds()

	mw.m.RecordBatch(
		r.Context(),
		mw.m.Labels(
			pathKey.String(r.URL.Path),
			statusKey.Int(rw.Code()),
		),
		mw.latency.Measurement(requestDuration),
		mw.count.Measurement(1),
	)
}
