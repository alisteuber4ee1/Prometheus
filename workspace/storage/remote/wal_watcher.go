package remote

import (
	"context"
	"sync"
	"time"
)

type WALReader interface {
	ReadNext() ([]Sample, int, error)
}

type WALWatcher struct {
	qm             *QueueManager
	reader         WALReader
	lastAckSegment int
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
}

func NewWALWatcher(qm *QueueManager, reader WALReader) *WALWatcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &WALWatcher{
		qm:     qm,
		reader: reader,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (w *WALWatcher) Start() {
	w.wg.Add(1)
	go w.run()
}

func (w *WALWatcher) Stop() {
	w.cancel()
	w.wg.Wait()
}

func (w *WALWatcher) run() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			samples, segment, err := w.reader.ReadNext()
			if err != nil {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			if len(samples) == 0 {
				time.Sleep(10 * time.Millisecond)
				continue
			}

			for {
				if w.qm.Append(samples) {
					w.lastAckSegment = segment
					break
				}
				select {
				case <-w.ctx.Done():
					return
				default:
					time.Sleep(10 * time.Millisecond)
				}
			}
		}
	}
}

func (w *WALWatcher) LastAckSegment() int {
	return w.lastAckSegment
}
