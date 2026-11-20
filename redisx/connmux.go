// Copyright 2014 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package redisx

import (
	"context"
	"errors"
	"sync"

	"github.com/gomodule/redigo/redis"
)

// ErrConnMuxClosed is returned by a mux connection after the ConnMux has
// been closed.
var ErrConnMuxClosed = errors.New("redisx: connection mux is closed")

// errConnPoisoned is delivered to the commands that were still in flight when
// another command forced the underlying connection to be torn down.
var errConnPoisoned = errors.New("redisx: underlying connection poisoned by a cancelled or failed command")

// errUnderlyingClosed mirrors the unexported sentinel used by the redigo
// driver when its connection has been closed. The mux rewrites it to its own
// terminal error so callers never depend on a driver-internal string.
var errUnderlyingClosed = errors.New("redigo: closed")

// ConnMuxStats contains statistics about the commands multiplexed on a
// ConnMux. Every registered command reaches exactly one of the Success,
// Cancelled or Error terminal states.
type ConnMuxStats struct {
	// Sent is the total number of commands handed to the underlying
	// connection for writing. Commands cancelled before they are written
	// are not counted here.
	Sent int64
	// Success is the number of commands whose full reply was received.
	Success int64
	// Cancelled is the number of commands abandoned because the caller's
	// context was cancelled or expired, either while queued or while the
	// reply was being read. Such commands are never counted as success.
	Cancelled int64
	// Error is the number of commands that failed for a reason other than
	// cancellation (a broken or poisoned underlying connection, a write
	// error, a protocol error, ...).
	Error int64
}

// ConnMux multiplexes one or more connections to a single underlying
// connection. The ConnMux connections do not support concurrency, commands
// that associate server side state with the connection or commands that put
// the connection in a special mode.
//
// Commands are serialized on the underlying connection in the order they are
// written. Each command has an explicit cancellation boundary: a command that
// is cancelled before it is written is never sent, while a command that was
// written but whose reply cannot be read to completion poisons the underlying
// connection, because the partially consumed RESP message cannot be skipped
// without risking reply misalignment for the next waiter.
type ConnMux struct {
	c redis.Conn

	// writeTicket is a one-capacity channel acting as a context-aware
	// mutex serializing every write to the underlying connection.
	writeTicket chan struct{}
	sendID      uint

	recvMu sync.Mutex
	// recvID is the id of the next reply to read from the underlying
	// connection. It only advances after a full RESP message has been read,
	// so replies never cross a message boundary.
	recvID uint
	// inflight maps the id of every registered command to its request slot.
	// A slot is removed exactly once, when the command reaches a terminal
	// state: a full reply, a cancellation or a connection-wide failure.
	inflight map[uint]*request
	// poisonErr is set when the underlying connection is poisoned; every
	// slot lookup that misses afterwards returns this error.
	poisonErr error

	closeOnce sync.Once
	closed    chan struct{}

	statsMu sync.Mutex
	stats   ConnMuxStats
}

type requestState int

const (
	requestWaiting   requestState = iota // queued or reply being read
	requestCancelled                     // caller gave up; the connection must be (or has been) poisoned
	requestDone                          // reply or terminal error recorded
)

// request is the bookkeeping for a single multiplexed command.
type request struct {
	done chan struct{}
	// turn is closed when the reply stream reaches this command's id, i.e.
	// when every earlier command has been fully read. It is closed at most
	// once via turnOnce, so a waiter queued behind another command is woken
	// exactly when its turn at the read head arrives.
	turn     chan struct{}
	turnOnce sync.Once

	mu    sync.Mutex
	state requestState
	reply interface{}
	err   error
}

func newRequest() *request {
	return &request{
		done: make(chan struct{}),
		turn: make(chan struct{}),
	}
}

// signalTurn wakes a waiter blocked until the reply stream reaches this
// command's id. Safe to call multiple times.
func (r *request) signalTurn() {
	r.turnOnce.Do(func() { close(r.turn) })
}

// finish records the terminal outcome of the request and closes its done
// channel. It is safe to call concurrently with cancel; exactly one of the two
// wins.
func (r *request) finish(reply interface{}, err error) bool {
	r.mu.Lock()
	if r.state != requestWaiting {
		r.mu.Unlock()
		return false
	}
	r.state = requestDone
	r.reply = reply
	r.err = err
	r.mu.Unlock()
	close(r.done)
	return true
}

// cancel marks the request as cancelled and wakes up anybody waiting on it.
// It returns true when the caller owns the cancellation, false if the request
// had already reached a terminal state.
func (r *request) cancel() bool {
	r.mu.Lock()
	if r.state != requestWaiting {
		r.mu.Unlock()
		return false
	}
	r.state = requestCancelled
	r.mu.Unlock()
	close(r.done)
	return true
}

// result returns the recorded outcome. It never returns a nil error: a slot
// observed as cancelled reports context.Canceled so waiters cannot receive a
// zero-value reply.
func (r *request) result() (interface{}, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == requestCancelled {
		return nil, context.Canceled
	}
	return r.reply, r.err
}

func NewConnMux(c redis.Conn) *ConnMux {
	ticket := make(chan struct{}, 1)
	ticket <- struct{}{}
	return &ConnMux{
		c:           c,
		writeTicket: ticket,
		inflight:    make(map[uint]*request),
		closed:      make(chan struct{}),
	}
}

// Get gets a connection. The application must close the returned connection.
func (p *ConnMux) Get() redis.Conn {
	c := &muxConn{p: p}
	c.ids = c.buf[:0]
	return c
}

// Close closes the underlying connection. All commands still in flight fail
// with ErrConnMuxClosed and concurrent Close calls converge on a single
// shutdown.
func (p *ConnMux) Close() error {
	p.closeOnce.Do(func() {
		close(p.closed)
		// Mark every in-flight command as failed and close the underlying
		// connection first. poisonLocked does not take the write turn: a
		// writer stuck inside an underlying write is only unblocked by the
		// Close below, and re-checks poisonErr as soon as the write returns.
		p.poisonLocked(ErrConnMuxClosed)
		// Shut the underlying connection down. Any error it returns here has
		// already been recorded as the poison cause and delivered to the
		// affected waiters; Close itself converges cleanly and exactly once.
		p.c.Close()
	})
	return nil
}

// Stats returns a snapshot of the multiplexer command counters.
func (p *ConnMux) Stats() ConnMuxStats {
	p.statsMu.Lock()
	s := p.stats
	p.statsMu.Unlock()
	return s
}

func (p *ConnMux) addStat(delta func(*ConnMuxStats)) {
	p.statsMu.Lock()
	delta(&p.stats)
	p.statsMu.Unlock()
}

// poisonError returns the terminal error of the mux, or nil while it is
// still healthy.
func (p *ConnMux) poisonError() error {
	p.recvMu.Lock()
	err := p.poisonErr
	p.recvMu.Unlock()
	return err
}

// acquireWrite waits for the shared write turn, bounded by ctx. Queueing for
// the write turn is part of the command's lifetime and therefore shares the
// same deadline as the read/write itself.
func (p *ConnMux) acquireWrite(ctx context.Context) error {
	// Fail fast once the mux has been poisoned or closed, even if another
	// goroutine is still holding the turn while its underlying write unwinds.
	p.recvMu.Lock()
	poisonErr := p.poisonErr
	p.recvMu.Unlock()
	if poisonErr != nil {
		return poisonErr
	}
	select {
	case <-p.writeTicket:
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closed:
		return ErrConnMuxClosed
	}
	p.recvMu.Lock()
	poisonErr = p.poisonErr
	p.recvMu.Unlock()
	if poisonErr != nil {
		p.releaseWrite()
		return poisonErr
	}
	return nil
}

func (p *ConnMux) releaseWrite() {
	p.writeTicket <- struct{}{}
}

// poison fails every command still in flight and marks the underlying
// connection as unusable. It is the only safe recovery when a RESP message
// cannot be consumed up to its boundary (a cancelled or timed-out read, a
// protocol error, ...): leaving the connection alive would hand the stale
// partial reply to the next waiter.
//
// poison never takes the write turn and never blocks: a writer holding the
// turn may be stuck inside the underlying write itself, in which case only the
// Close below can unblock it. Writers re-check poisonErr after every write
// operation and abort without registering anything new.
func (p *ConnMux) poison(err error) {
	p.poisonLocked(err)
}

func (p *ConnMux) poisonLocked(err error) {
	if err != nil && err.Error() == errUnderlyingClosed.Error() {
		err = ErrConnMuxClosed
	}
	p.recvMu.Lock()
	if p.poisonErr == nil {
		p.poisonErr = err
	}
	requests := p.inflight
	p.inflight = make(map[uint]*request)
	p.recvMu.Unlock()

	for _, r := range requests {
		// Wake any waiter still queued for its turn so it observes the
		// terminal failure instead of blocking forever.
		r.signalTurn()
		// Slots already cancelled are accounted for by their own caller;
		// everyone else terminates here as an error.
		if r.finish(nil, err) {
			p.addStat(func(s *ConnMuxStats) { s.Error++ })
		}
	}

	// Close the underlying connection to unblock any concurrent read or
	// write and to make subsequent calls fail fast. A connection poisoned
	// by an actual read/write error is usually already dead, and closing an
	// already closed redigo connection is a no-op.
	p.c.Close()
}

type muxConn struct {
	p   *ConnMux
	ids []uint
	buf [8]uint
}

var (
	_ redis.Conn            = (*muxConn)(nil)
	_ redis.ConnWithContext = (*muxConn)(nil)
)

// register assigns the next command id, reserves its request slot and writes
// the command through the underlying connection. The write turn must be held.
// A command whose context is already done is refused here so that it is never
// written. On a write failure the caller must not release the write turn:
// register poisons the mux and the ticket stays withdrawn.
func (c *muxConn) register(ctx context.Context, flush bool, cmd string, args ...interface{}) (ok bool, err error) {
	if err := ctx.Err(); err != nil {
		return true, err
	}
	p := c.p
	id := p.sendID
	r := newRequest()
	p.recvMu.Lock()
	p.inflight[id] = r
	p.recvMu.Unlock()

	c.ids = append(c.ids, id)
	p.sendID++

	writeErr := p.c.Send(cmd, args...)
	if writeErr == nil && flush {
		writeErr = p.c.Flush()
	}
	if writeErr != nil {
		// The write failed and the underlying connection is already fatal:
		// withdraw our own slot, then poison every other in-flight command.
		// The write turn is held here, so poisonLocked does not take it again.
		p.recvMu.Lock()
		delete(p.inflight, id)
		c.dropLastID(id)
		p.recvMu.Unlock()
		p.poisonLocked(writeErr)
		p.addStat(func(s *ConnMuxStats) { s.Error++ })
		return false, writeErr
	}
	// Another goroutine may have poisoned the mux while we were writing.
	// Do not advertise this command as a healthy send.
	p.recvMu.Lock()
	poisonErr := p.poisonErr
	p.recvMu.Unlock()
	if poisonErr != nil {
		p.recvMu.Lock()
		delete(p.inflight, id)
		c.dropLastID(id)
		p.recvMu.Unlock()
		return false, c.terminalError(poisonErr)
	}
	p.addStat(func(s *ConnMuxStats) { s.Sent++ })
	return true, nil
}

// dropLastID removes the id most recently appended to c.ids. A mux connection
// is not used concurrently, so the append/withrollback pair is only reached by
// the owning goroutine.
func (c *muxConn) dropLastID(id uint) {
	if n := len(c.ids); n > 0 && c.ids[n-1] == id {
		c.ids = c.ids[:n-1]
		if len(c.ids) == 0 {
			c.ids = c.buf[:0]
		}
	}
}

func (c *muxConn) sendContext(ctx context.Context, flush bool, cmd string, args ...interface{}) error {
	if lookupCommandInfo(cmd).notMuxable {
		return errors.New("command not supported by mux pool")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p := c.p
	if err := p.acquireWrite(ctx); err != nil {
		return err
	}
	// Re-check while holding the write turn: the context may have expired
	// while queued behind another writer. Nothing has been written in that
	// case, so the command is simply dropped.
	release, err := c.register(ctx, flush, cmd, args...)
	if release {
		p.releaseWrite()
	}
	return err
}

func (c *muxConn) send(flush bool, cmd string, args ...interface{}) error {
	return c.sendContext(context.Background(), flush, cmd, args...)
}

func (c *muxConn) Send(cmd string, args ...interface{}) error {
	return c.send(false, cmd, args...)
}

// SendContext writes the command to the output buffer without flushing. The
// context bounds the wait for the shared write turn; a cancelled context
// before the command is written prevents it from being sent at all.
func (c *muxConn) SendContext(ctx context.Context, cmd string, args ...interface{}) error {
	return c.sendContext(ctx, false, cmd, args...)
}

func (c *muxConn) Flush() error {
	return c.FlushContext(context.Background())
}

// FlushContext flushes the output buffer, bounded by ctx. A failure while
// flushing poisons the underlying connection because an unknown prefix of the
// buffer may have reached the server; the write turn stays withdrawn.
func (c *muxConn) FlushContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p := c.p
	if err := p.acquireWrite(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		p.releaseWrite()
		return err
	}
	if err := p.c.Flush(); err != nil {
		p.poisonLocked(err)
		return err
	}
	p.recvMu.Lock()
	poisonErr := p.poisonErr
	p.recvMu.Unlock()
	if poisonErr != nil {
		return c.terminalError(poisonErr)
	}
	p.releaseWrite()
	return nil
}

// popID removes and returns the next id whose reply this connection is
// responsible for.
func (c *muxConn) popID() (uint, error) {
	if len(c.ids) == 0 {
		return 0, errors.New("mux pool underflow")
	}
	id := c.ids[0]
	c.ids = c.ids[1:]
	if len(c.ids) == 0 {
		c.ids = c.buf[:0]
	}
	return id, nil
}

// terminalError rewrites errors produced by an underlying connection that was
// torn down by the mux itself into the mux-level terminal error, so waiters
// always observe a stable mux error rather than the driver's internal
// "redigo: closed" string.
func (c *muxConn) terminalError(err error) error {
	if err == nil {
		return nil
	}
	if pErr := c.p.poisonError(); pErr != nil {
		return pErr
	}
	if err.Error() == errUnderlyingClosed.Error() {
		return ErrConnMuxClosed
	}
	return err
}

func (c *muxConn) receive(ctx context.Context) (interface{}, error) {
	if err := ctx.Err(); err != nil {
		// Do not consume an id: the caller may retry the Receive with a
		// fresh context, or Close the connection and drain.
		return nil, err
	}
	id, err := c.popID()
	if err != nil {
		return nil, err
	}

	p := c.p
	p.recvMu.Lock()
	r := p.inflight[id]
	poisonErr := p.poisonErr
	p.recvMu.Unlock()
	if r == nil {
		// The slot was taken by a poison event (or by Close) before we got
		// here. Report that terminal error instead of touching the reply
		// stream, which no longer sits on a known message boundary.
		if poisonErr != nil {
			return nil, c.terminalError(poisonErr)
		}
		panic("mux pool: receive for unknown command id")
	}

	// Wait for our turn at the reply stream. Replies are read strictly in
	// send order, so a waiter for an earlier id must finish first.
	p.recvMu.Lock()
	ourTurn := p.recvID == id
	p.recvMu.Unlock()
	if !ourTurn {
		select {
		case <-r.turn:
			// The read head reached our id because every earlier reply was
			// consumed up to its RESP message boundary. Re-check under the
			// lock to rule out a lost wakeup racing with a poison.
			p.recvMu.Lock()
			ourTurn = p.recvID == id
			terminal := p.poisonErr
			p.recvMu.Unlock()
			if !ourTurn {
				if terminal != nil {
					return nil, c.terminalError(terminal)
				}
				return nil, errors.New("mux pool: spurious turn signal")
			}
		case <-r.done:
			// The command was cancelled or failed while queued. The
			// connection has already been poisoned when required, so there
			// is no reply to align to.
			v, resErr := r.result()
			return v, c.terminalError(resErr)
		case <-p.closed:
			return nil, ErrConnMuxClosed
		case <-ctx.Done():
			// Cancelled while still queued. The command was already
			// written but its reply has not been consumed, so the
			// connection must be invalidated: no later command may read a
			// reply that belongs to this one.
			if r.cancel() {
				p.addStat(func(s *ConnMuxStats) { s.Cancelled++ })
			}
			p.poison(errConnPoisoned)
			return nil, ctx.Err()
		}
	}

	// This request owns the read head of the stream. The underlying read
	// honors the same deadline as the queue wait above. If the underlying
	// connection cannot be driven with a context, fall back to a plain
	// Receive; the ConnMux serialization still guarantees ordering.
	var (
		reply  interface{}
		rcvErr error
	)
	if cwc, ok := p.c.(redis.ConnWithContext); ok {
		reply, rcvErr = cwc.ReceiveContext(ctx)
	} else {
		reply, rcvErr = p.c.Receive()
	}

	p.recvMu.Lock()
	if rcvErr != nil {
		delete(p.inflight, id)
		p.recvMu.Unlock()
		// The read failed mid-message (timeout, cancellation, network or
		// protocol error). The read head is not on a RESP boundary, so the
		// connection must not be reused.
		if errors.Is(rcvErr, context.Canceled) || errors.Is(rcvErr, context.DeadlineExceeded) {
			if r.cancel() {
				p.addStat(func(s *ConnMuxStats) { s.Cancelled++ })
			}
		} else if r.finish(nil, rcvErr) {
			p.addStat(func(s *ConnMuxStats) { s.Error++ })
		}
		p.poison(rcvErr)
		return nil, c.terminalError(rcvErr)
	}

	// A full RESP message was read. Advance the read head and hand the
	// reply to whoever owns this id.
	p.recvID = id + 1
	delete(p.inflight, id)
	next := p.inflight[id+1]
	p.recvMu.Unlock()

	// Wake the waiter for the next command, if it is already registered and
	// queued. The signal is the only path that lets a blocked reader start,
	// so it must fire after the read head advances and outside the lock.
	if next != nil {
		next.signalTurn()
	}

	if !r.finish(reply, nil) {
		// The slot already reached a terminal state while the read was in
		// flight: a concurrent poison finished it and accounted for it as an
		// error. The whole message was consumed so the stream stays aligned,
		// but the reply must not be delivered as a success and no extra
		// counter is added here.
		v, resErr := r.result()
		return v, c.terminalError(resErr)
	}
	p.addStat(func(s *ConnMuxStats) { s.Success++ })
	return reply, nil
}

func (c *muxConn) Receive() (interface{}, error) {
	return c.receive(context.Background())
}

// ReceiveContext receives a single reply, bounded by ctx. Cancelling ctx while
// queued or reading fails the command and, once its reply may have been
// partially consumed, poisons the underlying connection.
func (c *muxConn) ReceiveContext(ctx context.Context) (interface{}, error) {
	return c.receive(ctx)
}

func (c *muxConn) Close() error {
	if len(c.ids) == 0 {
		// Even with nothing to drain, surface a poisoned/closed mux so
		// callers cannot observe a healthy connection after shutdown.
		return c.terminalError(c.p.poisonError())
	}
	// Drain the replies still owned by this connection so that later mux
	// connections are not blocked behind them. A poisoned or closed
	// connection ends the drain immediately with the terminal error.
	var err error
	if e := c.Flush(); e != nil {
		err = e
	}
	for len(c.ids) > 0 {
		if _, e := c.Receive(); e != nil {
			if err == nil {
				err = e
			}
			break
		}
	}
	// The remaining ids have all been resolved (or are about to be) by the
	// poison/close path in the mux. Drop them locally so repeated Close
	// calls keep returning the terminal error.
	c.ids = c.buf[:0]
	if err == nil {
		err = c.p.poisonError()
	}
	return c.terminalError(err)
}

func (c *muxConn) doContext(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	if err := c.sendContext(ctx, true, cmd, args...); err != nil {
		return nil, err
	}
	return c.receive(ctx)
}

func (c *muxConn) Do(cmd string, args ...interface{}) (interface{}, error) {
	return c.doContext(context.Background(), cmd, args...)
}

// DoContext sends a command and receives its reply under a single context
// deadline that covers queueing, writing and reading.
func (c *muxConn) DoContext(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	return c.doContext(ctx, cmd, args...)
}

func (c *muxConn) Err() error {
	select {
	case <-c.p.closed:
		return ErrConnMuxClosed
	default:
	}
	p := c.p
	p.recvMu.Lock()
	err := p.poisonErr
	p.recvMu.Unlock()
	if err != nil {
		return err
	}
	return c.p.c.Err()
}
