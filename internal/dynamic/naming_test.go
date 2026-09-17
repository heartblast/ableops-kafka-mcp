package dynamic

import "testing"

func TestToolName(t *testing.T) {
	for id, want := range map[string]string{
		"listClusters":              "list_clusters",
		"getConsumerGroupLag":       "get_consumer_group_lag",
		"listEventOccurrences":      "list_event_occurrences",
		"getClusterPartitionHealth": "get_cluster_partition_health",
		"getTopic":                  "get_topic",
		"getMe":                     "get_me",
		"login":                     "login",
		"getHTTPStatus":             "get_http_status",
		"getURLPath":                "get_url_path",
		"getV2Topic":                "get_v2_topic",
		"listTopicsV2":              "list_topics_v2",
		"getS3Bucket":               "get_s3_bucket",
		"getHTTP2Settings":          "get_http2_settings",
		"getAPI":                    "get_api",
	} {
		got, err := ToolName(id)
		if err != nil || got != want {
			t.Errorf("ToolName(%q)=%q,%v want %q", id, got, err, want)
		}
	}
	for _, id := range []string{"", "ListClusters", "list_clusters", "list-clusters", "1list", "list clusters", "목록조회", "a" + string(make([]byte, 128))} {
		if got, err := ToolName(id); err == nil {
			t.Errorf("ToolName(%q)=%q, want error", id, got)
		}
	}
}
