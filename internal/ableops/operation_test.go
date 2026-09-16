package ableops

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOperationCallBudgetAndIsolation(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, `{}`)
	}))
	defer server.Close()
	client := mockClient(t, server)
	ctx, cancel := client.BeginOperation(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	var limited atomic.Int32
	for range MaxOperationRequests + 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var out struct{}
			if err := client.Get(ctx, []string{"synthetic"}, nil, &out); err != nil {
				requireErrorCode(t, err, "call_limit_exceeded")
				limited.Add(1)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != MaxOperationRequests || limited.Load() != 3 {
		t.Fatal("동시 하위 요청 수 상한이 적용되지 않았습니다")
	}
	other, otherCancel := client.BeginOperation(context.Background())
	defer otherCancel()
	var out struct{}
	if err := client.Get(other, []string{"synthetic"}, nil, &out); err != nil {
		t.Fatal("다른 요청과 호출 예산이 공유되었습니다")
	}
}

func TestOperationUsesOneDeadlineAndPropagatesCancellation(t *testing.T) {
	first := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/first" {
			close(first)
			fmt.Fprint(w, `{}`)
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	client := mockClient(t, server)
	// 짧은 예산을 주입하여 동일한 마감 시각을 후속 호출도 사용하는지 검증한다.
	client.timeout = 80 * time.Millisecond
	ctx, cancel := client.BeginOperation(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("전체 마감 시각이 없습니다")
	}
	var out struct{}
	if err := client.Get(ctx, []string{"first"}, nil, &out); err != nil {
		t.Fatal(err)
	}
	<-first
	requireErrorCode(t, client.Get(ctx, []string{"second"}, nil, &out), "timeout")
	if current, _ := ctx.Deadline(); current != deadline {
		t.Fatal("후속 호출에서 전체 마감 시각이 연장되었습니다")
	}
	canceled, stop := client.BeginOperation(context.Background())
	stop()
	requireErrorCode(t, client.Get(canceled, []string{"never"}, nil, &out), "canceled")
}

func TestOptionalGetOnlyAcceptsContractualNoContent(t *testing.T) {
	for _, status := range []int{http.StatusNoContent, http.StatusOK, http.StatusForbidden, http.StatusNotFound} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}))
			defer server.Close()
			client := mockClient(t, server)
			var out struct{}
			present, err := client.GetOptional(context.Background(), []string{"synthetic"}, nil, &out)
			if present {
				t.Fatal("본문 없는 응답이 상세 데이터로 처리되었습니다")
			}
			if status == http.StatusNoContent {
				if err != nil {
					t.Fatal(err)
				}
				requireErrorCode(t, client.Get(context.Background(), []string{"synthetic"}, nil, &out), "invalid_response")
			} else {
				want := map[int]string{200: "invalid_response", 403: "access_denied", 404: "not_found"}[status]
				requireErrorCode(t, err, want)
			}
		})
	}
}
