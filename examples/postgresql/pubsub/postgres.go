package pubsub

import (
	"context"
	"fmt"
	"sync"

	"github.com/Liphium/hydro"
	"github.com/lib/pq"
	"gorm.io/gorm"
)

type PostgresPubSub struct {
	connStr string
}

func NewPostgresPubSub(connStr string) *PostgresPubSub {
	return &PostgresPubSub{connStr: connStr}
}

func (p *PostgresPubSub) Publish(ctx context.Context, db *gorm.DB, channel string, message string) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	
	query := fmt.Sprintf("NOTIFY %s, %s", pq.QuoteIdentifier(channel), pq.QuoteLiteral(message))
	_, err = sqlDB.ExecContext(ctx, query)
	return err
}

type PostgresWorker struct {
	connStr  string
	listener *pq.Listener
	onMsg    func(channel string, message string)
	onErr    func(channel string, err error)
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func (p *PostgresPubSub) CreateWorker() hydro.ISubWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &PostgresWorker{
		connStr: p.connStr,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (w *PostgresWorker) Subscribe(ctx context.Context, channels ...string) error {
	if w.listener == nil {
		w.listener = pq.NewListener(w.connStr, 0, 0, w.handleEvent)
		w.wg.Add(1)
		go w.listenLoop()
	}
	for _, ch := range channels {
		if err := w.listener.Listen(ch); err != nil {
			return err
		}
	}
	return nil
}

func (w *PostgresWorker) Unsubscribe(ctx context.Context, channels ...string) error {
	if w.listener == nil {
		return nil
	}
	for _, ch := range channels {
		if err := w.listener.Unlisten(ch); err != nil {
			return err
		}
	}
	return nil
}

func (w *PostgresWorker) OnMessage(fn func(channel string, message string)) {
	w.onMsg = fn
}

func (w *PostgresWorker) OnError(fn func(channel string, err error)) {
	w.onErr = fn
}

func (w *PostgresWorker) Close() {
	w.cancel()
	if w.listener != nil {
		w.listener.Close()
	}
	w.wg.Wait()
}

func (w *PostgresWorker) handleEvent(ev pq.ListenerEventType, err error) {
	if err != nil && w.onErr != nil {
		w.onErr("", err)
	}
}

func (w *PostgresWorker) listenLoop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case notification := <-w.listener.Notify:
			if notification == nil {
				continue
			}
			if w.onMsg != nil {
				w.onMsg(notification.Channel, notification.Extra)
			}
		}
	}
}
