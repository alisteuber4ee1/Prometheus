package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	samplesDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_remote_storage_samples_dropped_total",
		Help: "Total number of samples dropped.",
	})
)

type Sample struct {
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
}

type QueueManager struct {
	cfg                QueueConfig
	client             *http.Client
	endpoint           string
	samplesChan        chan []Sample
	shards             []*shard
	shardsMtx          sync.RWMutex
	numShards          int
	droppedTotal       int64
	wg                 sync.WaitGroup
	ctx                context.Context
	cancel             context.CancelFunc

	maxBufferedSamples int
	bufferedSamples    int
	bufferedMtx        sync.Mutex
	bufferedCond       *sync.Cond
}

type QueueConfig struct {
	MaxSamplesPerSend int
	BatchSendDeadline time.Duration
	MaxBackoff        time.Duration
}

type shard struct {
	qm          *QueueManager
	mtx         sync.Mutex
	queue       []Sample
	activeBatch []Sample
	wakeup      chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	stopped     bool
}

func NewQueueManager(cfg QueueConfig, endpoint string, numShards int, maxBufferedSamples int) *QueueManager {
	if maxBufferedSamples <= 0 {
		maxBufferedSamples = 10000
	}
	ctx, cancel := context.WithCancel(context.Background())
	qm := &QueueManager{
		cfg:                cfg,
		client:             &http.Client{Timeout: 5 * time.Second},
		endpoint:           endpoint,
		samplesChan:        make(chan []Sample, 1000),
		numShards:          numShards,
		maxBufferedSamples: maxBufferedSamples,
		ctx:                ctx,
		cancel:             cancel,
	}
	qm.bufferedCond = sync.NewCond(&qm.bufferedMtx)
	return qm
}

func (qm *QueueManager) Start() {
	qm.shardsMtx.Lock()
	defer qm.shardsMtx.Unlock()
	qm.shards = make([]*shard, qm.numShards)
	for i := 0; i < qm.numShards; i++ {
		qm.shards[i] = qm.newShard()
		qm.shards[i].start()
	}
	qm.wg.Add(1)
	go qm.routeSamples()
}

func (qm *QueueManager) Stop() {
	qm.cancel()
	qm.bufferedMtx.Lock()
	qm.bufferedCond.Broadcast()
	qm.bufferedMtx.Unlock()

	qm.wg.Wait()

	qm.shardsMtx.Lock()
	for _, s := range qm.shards {
		s.stopAndDrain()
	}
	qm.shardsMtx.Unlock()
}

func (qm *QueueManager) Append(samples []Sample) bool {
	n := len(samples)
	if n == 0 {
		return true
	}

	qm.bufferedMtx.Lock()
	for qm.bufferedSamples+n > qm.maxBufferedSamples && qm.ctx.Err() == nil {
		qm.bufferedCond.Wait()
	}
	if qm.ctx.Err() != nil {
		qm.bufferedMtx.Unlock()
		return false
	}
	qm.bufferedSamples += n
	qm.bufferedMtx.Unlock()

	select {
	case qm.samplesChan <- samples:
		return true
	case <-qm.ctx.Done():
		qm.bufferedMtx.Lock()
		qm.bufferedSamples -= n
		qm.bufferedCond.Broadcast()
		qm.bufferedMtx.Unlock()
		return false
	}
}

func (qm *QueueManager) routeSamples() {
	defer qm.wg.Done()
	for {
		select {
		case batch, ok := <-qm.samplesChan:
			if !ok {
				return
			}
			qm.routeBatch(batch)
		case <-qm.ctx.Done():
			for {
				select {
				case batch, ok := <-qm.samplesChan:
					if !ok {
						return
					}
					qm.routeBatch(batch)
				default:
					return
				}
			}
		}
	}
}

func (qm *QueueManager) routeBatch(batch []Sample) {
	qm.shardsMtx.RLock()
	defer qm.shardsMtx.RUnlock()
	if len(qm.shards) == 0 {
		return
	}
	for _, s := range batch {
		shardIdx := int(s.Timestamp) % len(qm.shards)
		if shardIdx < 0 {
			shardIdx = -shardIdx
		}
		qm.shards[shardIdx].push(s)
	}
}

func (qm *QueueManager) Reshard(newNumShards int) {
	qm.shardsMtx.Lock()
	defer qm.shardsMtx.Unlock()

	if newNumShards == qm.numShards {
		return
	}

	var buffered []Sample
	for _, s := range qm.shards {
		samples := s.stopAndDrain()
		buffered = append(buffered, samples...)
	}

	qm.numShards = newNumShards
	qm.shards = make([]*shard, qm.numShards)
	for i := 0; i < qm.numShards; i++ {
		qm.shards[i] = qm.newShard()
		qm.shards[i].start()
	}

	for _, s := range buffered {
		shardIdx := int(s.Timestamp) % qm.numShards
		if shardIdx < 0 {
			shardIdx = -shardIdx
		}
		qm.shards[shardIdx].push(s)
	}
}

func (qm *QueueManager) decrementBuffered(n int) {
	qm.bufferedMtx.Lock()
	qm.bufferedSamples -= n
	qm.bufferedCond.Broadcast()
	qm.bufferedMtx.Unlock()
}

func (qm *QueueManager) newShard() *shard {
	ctx, cancel := context.WithCancel(qm.ctx)
	return &shard{
		qm:     qm,
		wakeup: make(chan struct{}, 1),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s *shard) start() {
	s.wg.Add(1)
	go s.run()
}

func (s *shard) push(sample Sample) {
	s.mtx.Lock()
	if !s.stopped {
		s.queue = append(s.queue, sample)
		select {
		case s.wakeup <- struct{}{}:
		default:
		}
	}
	s.mtx.Unlock()
}

func (s *shard) stopAndDrain() []Sample {
	s.mtx.Lock()
	s.stopped = true
	s.mtx.Unlock()

	s.cancel()
	s.wg.Wait()

	s.mtx.Lock()
	defer s.mtx.Unlock()

	var samples []Sample
	呈现 := append(samples, s.activeBatch...)
	呈现 = append(呈现, s.queue...)
	s.activeBatch = nil
	s.queue = nil
	return 呈现
}

func (s *shard) run() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.qm.cfg.BatchSendDeadline)
	defer ticker.Stop()

	for {
		select {
		case <-s.wakeup:
			s.processQueue()
		case <-ticker.C:
			if len(s.activeBatch) > 0 {
				s.sendWithRetry()
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *shard) processQueue() {
	for {
		s.mtx.Lock()
		if len(s.queue) == 0 || s.stopped {
			s.mtx.Unlock()
			return
		}
		needed := s.qm.cfg.MaxSamplesPerSend - len(s.activeBatch)
		if needed <= 0 {
			s.mtx.Unlock()
			s.sendWithRetry()
			if s.ctx.Err() != nil {
				return
			}
			continue
		}

		n := needed
		if n > len(s.queue) {
			n = len(s.queue)
		}

		s.activeBatch = append(s.activeBatch, s.queue[:n]...)
		s.queue = s.queue[n:]
		s.mtx.Unlock()

		if len(s.activeBatch) >= s.qm.cfg.MaxSamplesPerSend {
			s.sendWithRetry()
			if s.ctx.Err() != nil {
				return
			}
		}
	}
}

func (s *shard) sendWithRetry() {
	backoff := 100 * time.Millisecond
	maxBackoff := s.qm.cfg.MaxBackoff
	if maxBackoff == 0 {
		maxBackoff = 5 * time.Second
	}

	for {
		if err := s.send(s.activeBatch); err == nil {
			n := len(s.activeBatch)
			s.activeBatch = nil
			s.qm.decrementBuffered(n)
			return
		}

		select {
		case <-s.ctx.Done():
			return
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func (s *shard) send(batch []Sample) error {
	data, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(s.ctx, "POST", s.qm.endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.qm.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("server returned HTTP status %s", resp.Status)
}
