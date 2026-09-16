package integration_test

import (
	"encoding/json"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// verifyAnalysisTools는 안전하게 주입된 대상만 조회한다. 값을 검색하거나 생성하지 않는다.
func verifyAnalysisTools(t *testing.T, cs *mcp.ClientSession, token, cluster string) {
	for _, tc := range []struct {
		name    string
		targets map[string]string
		include []string
	}{
		{"get_topic_detail", map[string]string{"topic_name": "ABLEOPS_VERIFY_TOPIC_NAME"}, []string{"configs", "partitions"}},
		{"get_consumer_group_members", map[string]string{"group_name": "ABLEOPS_VERIFY_GROUP_NAME"}, nil},
		{"get_event_detail", map[string]string{"event_id": "ABLEOPS_VERIFY_EVENT_ID"}, []string{"occurrences", "issue", "playbook"}},
		{"get_asset_impact", map[string]string{"asset_type": "ABLEOPS_VERIFY_ASSET_TYPE", "asset_key": "ABLEOPS_VERIFY_ASSET_KEY"}, []string{"impact"}},
		{"get_request_status", map[string]string{"request_id": "ABLEOPS_VERIFY_REQUEST_ID"}, []string{"history"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireCluster(t, cluster)
			args := map[string]any{"cluster_id": cluster, "limit": 10}
			for field, key := range tc.targets {
				args[field] = optionalTarget(t, key)
			}
			if tc.include != nil {
				args["include"] = tc.include
			}
			out := invoke[json.RawMessage](t, cs, tc.name, args, token)
			checkScope(t, out.ClusterID, cluster)
			if out.Data == nil {
				t.Error("제공한 대상의 필수 상세 조회 실패; 안전한 오류 코드를 확인하세요")
			}
		})
	}
}
