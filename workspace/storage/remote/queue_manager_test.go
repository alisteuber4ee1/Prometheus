package remote

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockWALReader struct {
	samples [][]Sample
	idx     int
	mtx     sync.Mutex
}

func (r *mockWALReader) ReadNext() ([]Sample, int, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	if r.idx >= len(r.samples) {
		return nil, r.idx, fmt.Errorf("EOF")
	}
	s := r.samples[r.idx]
	segment := r.idx
	r.idx++
	return s, segment, nil
}

func TestEndpointOutageAndRecovery(t *testing.T) {
	var (
		receivedSamples []Sample
		receivedMtx     sync.Mutex
		statusCode      int32 = http.StatusServiceUnavailable
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status := atomic.LoadInt32(&statusCode)
		if status != http.StatusOK {
			w.WriteHeader(int(status))
			return
		}

		var samples []Sample
		if err := json.NewDecoder(r.Body).Decode(&samples); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		receivedMtx.Lock()
		receivedSamples = append(receivedSamples, samples...)
		receivedMtx.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := QueueConfig{
		MaxSamplesPerSend: 10,
		BatchSendDeadline: 50 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
	}

	qm := NewQueueManager(cfg, server.URL, 2, 100)
	qm.Start()
	defer qm.Stop()

	var samples []Sample
	for i := 0; i < 50; i++ {
		samples = append(samples, Sample{Value: float64(i), Timestamp: int64(i)})
	}

	reader := &mockWALReader{
		samples: [][]Sample{samples},
	}

	watcher := NewWALWatcher(qm, reader)
	watcher.Start()
	defer watcher.Stop()

	time.Sleep(200 * time.Millisecond)

	receivedMtx.Lock()
	initialCount := len(receivedSamples)
	receivedMtx.Unlock()

	if initialCount != 0 {
		t.Fatalf("Expected 0 samples received during outage, got %d", initialCount)
	}

	atomic.StoreInt32(&statusCode, http.StatusOK)

	time.Sleep(500 * time.Millisecond)

	receivedMtx.Lock()
	finalCount := len(receivedSamples)
	receivedMtx.Unlock()

	if finalCount != 50 {
		t.Fatalf("Expected 50 samples received after recovery, got %d", finalCount)
	}
}

func TestReshardingDuringOutage(t *testing.T) {
	var (
		receivedSamples []Sample
		receivedMtx     sync.Mutex
		statusCode      int32 = http.StatusServiceUnavailable
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status := atomic.LoadInt32(&statusCode)
		if status != http.StatusOK {
			w.WriteHeader(int(status))
			return
		}

		var samples []Sample
		if err := json.NewDecoder(r.Body).Decode(&samples); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		receivedMtx.Lock()
		receivedSamples = append(receivedSamples, samples...)
		receivedMtx.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := QueueConfig{
		MaxSamplesPerSend: 10,
		BatchSendDeadline: 50 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
	}

	qm := NewQueueManager(cfg, server.URL, 2, 100)
	qm.Start()
	defer qm.Stop()

	var samples []Sample
	for i := 0; i < 50; i++ {
		samples = append(samples, Sample{Value: float64(i), Timestamp: int64(i)})
	}

	if ok := qm.Append(samples); !ok {
		t.Fatalf("Failed to append samples")
	}

	time.Sleep(200 * time.Millisecond)

	qm.Reshard(4)

	atomic.StoreInt32(&statusCode, http.StatusOK)

	time.Sleep(500 * time.Millisecond)

	receivedMtx.Lock()
	finalCount := len(receivedSamples)
	receivedMtx.Unlock()

	if finalCount != 50 {
		t.Fatalf("Expected 50 samples received after resharding and recovery, got %d", finalCount)
	}
}

func TestBackpressure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	cfg := QueueConfig{
		MaxSamplesPerSend: 10,
		BatchSendDeadline: 50 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
	}

	qm := NewQueueManager(cfg, server.URL, 2, 20)
	qm.Start()
	defer qm.Stop()

	var samples1 []Sample
	for i := 0; i < 20; i++ {
		samples1 = append(samples1, Sample{Value: float64(i), Timestamp: int64(i)})
	}
	if ok := qm.Append(samples1); !ok {
		t.Fatalf("Failed to append first batch")
	}

	done := make(chan bool)
	go func() {
		var samples2 []Sample
		for i := 20; i < 30; i++ {
			samples2 = append(samples2, Sample{Value: float64(i), Timestamp: int64(i)})
		}
		done <- qm.Append(samples2)
	}()

	select {
	case <-done:
		t.Fatalf("Append should have blocked due to backpressure")
	case <-time.After(200 * time.Millisecond):
		// Success: it blocked!
	}
}
