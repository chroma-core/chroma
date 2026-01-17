package embeddings

type DistanceMetric string

const (
	L2     DistanceMetric = "l2"
	COSINE DistanceMetric = "cosine"
	IP     DistanceMetric = "ip"
)

type DistanceMetricOperator interface {
	Compare(a, b []float32) float64
}

type Distance float32
type Distances []Distance
