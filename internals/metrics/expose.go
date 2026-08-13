package metrics

import (
	"fmt"
	"io"
	"math"
	"strconv"
)

// WriteCounter writes a single counter sample in Prometheus text
// exposition format. labels, if non-empty, must already be formatted as
// `key="value",key2="value2"` (no surrounding braces).
func WriteCounter(w io.Writer, name, help, labels string, value int64) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s%s %d\n", name, help, name, name, labelSuffix(labels), value)
}

// WriteGauge writes a single gauge sample in Prometheus text exposition
// format.
func WriteGauge(w io.Writer, name, help, labels string, value int64) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s%s %d\n", name, help, name, name, labelSuffix(labels), value)
}

// WriteHistogram writes a histogram snapshot (buckets, sum, count) in
// Prometheus text exposition format.
func WriteHistogram(w io.Writer, name, help, labels string, snap HistogramSnapshot) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
	for i, upper := range snap.BucketUpperBoundsSeconds {
		le := "+Inf"
		if !math.IsInf(upper, 1) {
			le = strconv.FormatFloat(upper, 'g', -1, 64)
		}
		fmt.Fprintf(w, "%s_bucket%s %d\n", name, labelSuffix(mergeLabels(labels, `le="`+le+`"`)), snap.CumulativeCounts[i])
	}
	fmt.Fprintf(w, "%s_sum%s %s\n", name, labelSuffix(labels), strconv.FormatFloat(snap.SumSeconds, 'g', -1, 64))
	fmt.Fprintf(w, "%s_count%s %d\n", name, labelSuffix(labels), snap.Count)
}

func labelSuffix(labels string) string {
	if labels == "" {
		return ""
	}
	return "{" + labels + "}"
}

func mergeLabels(base, extra string) string {
	if base == "" {
		return extra
	}
	return base + "," + extra
}
