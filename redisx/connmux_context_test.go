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

package redisx_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/gomodule/redigo/redisx"
	"github.com/stretchr/testify/require"
)

// fakeServer is a minimal, single-connection RESP server driven by a handler
// func. The handler receives the parsed command array and writes raw replies.
type fakeServer struct {
	ln       net.Listener
	gotCmds  chan []string
	closed   chan struct{}
	closeOne sync.Once
}

func startFakeServer(t *testing.T, handle func(net.Conn, []string, *bufio.Reader)) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := &fakeServer{
		ln:      ln,
		gotCmds: make(chan []string, 64),
		closed:  make(chan struct{}),
	}
	go func() {
		defer close(s.closed)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		br := bufio.NewReader(conn)
		for {
			cmd, err := readRESPArray(br)
			if err != nil {
				return
			}
			select {
			case s.gotCmds <- cmd:
			default:
			}
			handle(conn, cmd, br)
		}
	}()
	return s
}

func (s *fakeServer) addr() string { return s.ln.Addr().String() }

func (s *fakeServer) close() {
	s.closeOne.Do(func() { s.ln.Close() })
}

// dialMux dials the fake server and wraps it in a ConnMux.
func dialMux(t *testing.T, addr string, readTimeout time.Duration) *redisx.ConnMux {
	t.Helper()
	netConn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	c := redis.NewConn(netConn, readTimeout, time.Second)
	return redisx.NewConnMux(c)
}

func readRESPArray(br *bufio.Reader) ([]string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 || line[0] != '*' {
		return nil, fmt.Errorf("expected array, got %q", line)
	}
	n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		if _, err := br.ReadString('\n'); err != nil { // bulk length line
			return nil, err
		}
		s, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		out = append(out, strings.TrimRight(s, "\r\n"))
	}
	return out, nil
}

func writeBulk(conn net.Conn, s string) error {
	_, err := fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(s), s)
	return err
}

// echoHandler replies to ECHO with its argument, to PING with PONG.
func echoHandler(conn net.Conn, cmd []string, _ *bufio.Reader) {
	switch strings.ToUpper(cmd[0]) {
	case "ECHO":
		writeBulk(conn, cmd[1])
	case "PING":
		io.WriteString(conn, "+PONG\r\n")
	default:
		io.WriteString(conn, "+OK\r\n")
	}
}

// TestMuxContextBasic verifies that the context-aware path preserves normal
// request/reply behavior and ordering across several mux connections.
func TestMuxContextBasic(t *testing.T) {
	srv := startFakeServer(t, echoHandler)
	defer srv.close()
	m := dialMux(t, srv.addr(), time.Second)
	defer m.Close()

	require.Implements(t, (*redis.ConnWithContext)(nil), m.Get())

	ctx := context.Background()
	c1 := m.Get()
	c2 := m.Get()
	require.NoError(t, c1.Send("ECHO", "hello"))
	require.NoError(t, c2.Send("ECHO", "world"))
	require.NoError(t, c1.Flush())
	require.NoError(t, c2.Flush())

	v, err := redis.String(redis.ReceiveContext(c1, ctx))
	require.NoError(t, err)
	require.Equal(t, "hello", v)
	v, err = redis.String(redis.ReceiveContext(c2, ctx))
	require.NoError(t, err)
	require.Equal(t, "world", v)
	require.NoError(t, c1.Close())
	require.NoError(t, c2.Close())

	stats := m.Stats()
	require.Equal(t, int64(2), stats.Sent)
	require.Equal(t, int64(2), stats.Success)
	require.Equal(t, int64(0), stats.Cancelled)
	require.Equal(t, int64(0), stats.Error)
}

// TestMuxCancelBeforeWrite ensures a command whose context is already done is
// never written to the underlying connection, while a following command still
// runs normally.
func TestMuxCancelBeforeWrite(t *testing.T) {
	srv := startFakeServer(t, echoHandler)
	defer srv.close()
	m := dialMux(t, srv.addr(), time.Second)
	defer m.Close()

	c1 := m.Get()
	defer c1.Close()
	c2 := m.Get()
	defer c2.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c1.(redis.ConnWithContext).DoContext(ctx, "ECHO", "never-sent")
	require.ErrorIs(t, err, context.Canceled)

	// The cancelled command must not have reached the server.
	select {
	case cmd := <-srv.gotCmds:
		t.Fatalf("unexpected command on the wire: %v", cmd)
	default:
	}

	// A subsequent command on another connection must still work, proving
	// the stream was not desynchronized.
	v, err := redis.String(c2.(redis.ConnWithContext).DoContext(context.Background(), "ECHO", "ok"))
	require.NoError(t, err)
	require.Equal(t, "ok", v)

	stats := m.Stats()
	require.Equal(t, int64(1), stats.Sent)
	require.Equal(t, int64(1), stats.Success)
	require.Equal(t, int64(0), stats.Cancelled, "pre-write cancellation is not registered")
}

// blockingHandler holds the first command's reply until release is closed,
// then answers everything else normally. It is used to pin the read head so
// that later commands queue behind the slow one.
func blockingHandler(t *testing.T, release <-chan struct{}) func(net.Conn, []string, *bufio.Reader) {
	var once sync.Once
	return func(conn net.Conn, cmd []string, _ *bufio.Reader) {
		if strings.ToUpper(cmd[0]) == "ECHO" && cmd[1] == "slow" {
			once.Do(func() {
				<-release
			})
		}
		echoHandler(conn, cmd, nil)
	}
}

// TestMuxCancelWhileQueued covers requirement 3: a waiter cancelled while an
// earlier slow command still owns the read head must not cause reply
// misalignment. The cancelled command was already written, so the connection
// is poisoned and every later waiter gets an error rather than a stale reply.
func TestMuxCancelWhileQueued(t *testing.T) {
	release := make(chan struct{})
	srv := startFakeServer(t, blockingHandler(t, release))
	defer srv.close()
	m := dialMux(t, srv.addr(), 5*time.Second)

	slow := m.Get()
	queued := m.Get()
	after := m.Get()

	// Pin the read head with a command the server will not answer yet.
	require.NoError(t, slow.Send("ECHO", "slow"))
	require.NoError(t, slow.Flush())
	slowDone := make(chan struct{})
	go func() {
		slow.Receive()
		close(slowDone)
	}()

	// A second command is written and queued behind the slow one.
	require.NoError(t, queued.Send("ECHO", "queued"))
	require.NoError(t, queued.Flush())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err := queued.(redis.ConnWithContext).ReceiveContext(ctx)
	cancel()
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled), "got %v", err)

	// Wait for the cancelled slot to be fully reconciled.
	require.Eventually(t, func() bool {
		return m.Stats().Cancelled >= 1
	}, time.Second, 5*time.Millisecond)

	// Every later command must fail with the connection-wide failure rather
	// than receive the queued command's reply (the misalignment bug).
	_, err = after.(redis.ConnWithContext).DoContext(context.Background(), "ECHO", "after")
	require.Error(t, err)
	require.NotNil(t, err)

	// Release the slow command and close everything; the slow receive must
	// converge with an error because the connection was poisoned.
	close(release)
	select {
	case <-slowDone:
	case <-time.After(2 * time.Second):
		t.Fatal("slow Receive did not converge")
	}
	require.Error(t, slow.Close())
	require.Error(t, queued.Close())
	require.Error(t, after.Close())
	// The mux-wide close itself still reports a clean shutdown.
	require.NoError(t, m.Close())

	stats := m.Stats()
	require.Equal(t, int64(2), stats.Sent)
	require.Equal(t, int64(0), stats.Success, "cancelled commands must not count as success")
	require.Equal(t, int64(1), stats.Cancelled)
	require.GreaterOrEqual(t, stats.Error, int64(1))
}

// partialReplyHandler writes only the first line of a bulk-string reply for
// ECHO partial, then blocks. Receive therefore times out in the middle of a
// RESP message.
func partialReplyHandler(release <-chan struct{}) func(net.Conn, []string, *bufio.Reader) {
	var once sync.Once
	return func(conn net.Conn, cmd []string, _ *bufio.Reader) {
		if strings.ToUpper(cmd[0]) == "ECHO" && cmd[1] == "partial" {
			io.WriteString(conn, "$5\r\n")
			once.Do(func() { <-release })
			return
		}
		echoHandler(conn, cmd, nil)
	}
}

// TestMuxTimeoutAtReplyBoundary covers requirement 2's second half: when a
// timeout fires after some bytes of the reply were read, the underlying
// connection is poisoned so the truncated reply can never be handed to the
// next command.
func TestMuxTimeoutAtReplyBoundary(t *testing.T) {
	release := make(chan struct{})
	srv := startFakeServer(t, partialReplyHandler(release))
	defer srv.close()
	// The driver-level read timeout is comfortably longer than the command's
	// own context so the context normally fires first while the partial reply
	// is still incomplete. (Either deadline firing has the same poisoning
	// semantics, which is what this test asserts.)
	m := dialMux(t, srv.addr(), 10*time.Second)

	c1 := m.Get()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err := c1.(redis.ConnWithContext).DoContext(ctx, "ECHO", "partial")
	cancel()
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || isTimeout(err),
		"expected a deadline/timeout error, got %v", err)

	// The connection must now be unusable for everyone.
	c2 := m.Get()
	_, err = c2.(redis.ConnWithContext).DoContext(context.Background(), "PING")
	require.Error(t, err)
	require.Error(t, c2.Err())

	close(release)
	require.Error(t, c1.Close())
	require.Error(t, c2.Close())
	require.NoError(t, m.Close())

	stats := m.Stats()
	require.Equal(t, int64(1), stats.Sent)
	require.Equal(t, int64(0), stats.Success)
	require.Equal(t, int64(1), stats.Cancelled+stats.Error,
		"the timed-out command must terminate as cancelled or error, never success")
}

// isTimeout reports whether err is a network timeout, which is how the
// driver's own read deadline surfaces.
func isTimeout(err error) bool {
	var netErr interface{ Timeout() bool }
	return errors.As(err, &netErr) && netErr.Timeout()
}

// TestMuxDriverReadTimeoutPoison covers the same boundary when the driver's
// own read timeout (rather than the caller's context) fires mid-message. The
// command counts as an error, not a cancellation or success, and the mux is
// still poisoned.
func TestMuxDriverReadTimeoutPoison(t *testing.T) {
	release := make(chan struct{})
	srv := startFakeServer(t, partialReplyHandler(release))
	defer srv.close()
	m := dialMux(t, srv.addr(), 80*time.Millisecond)

	c1 := m.Get()
	_, err := c1.Do("ECHO", "partial")
	require.Error(t, err)

	c2 := m.Get()
	_, err = c2.Do("PING")
	require.Error(t, err)

	close(release)
	require.Error(t, c1.Close())
	require.Error(t, c2.Close())
	require.NoError(t, m.Close())

	stats := m.Stats()
	require.Equal(t, int64(1), stats.Sent)
	require.Equal(t, int64(0), stats.Success)
	require.Equal(t, int64(0), stats.Cancelled)
	require.Equal(t, int64(1), stats.Error)
}

// TestMuxCloseConvergesOnce covers requirement 4: Close racing with in-flight
// commands wakes every waiter with the closed error, never a zero-value reply,
// and a second Close is a harmless no-op.
func TestMuxCloseConvergesOnce(t *testing.T) {
	release := make(chan struct{})
	srv := startFakeServer(t, blockingHandler(t, release))
	defer srv.close()
	m := dialMux(t, srv.addr(), 5*time.Second)

	const n = 8
	conns := make([]redis.Conn, n)
	dones := make([]chan struct{}, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		conns[i] = m.Get()
		require.NoError(t, conns[i].Send("ECHO", "slow"))
		require.NoError(t, conns[i].Flush())
		dones[i] = make(chan struct{})
		go func(i int) {
			defer wg.Done()
			_, errs[i] = conns[i].Receive()
			close(dones[i])
		}(i)
	}

	// Give the first receive a moment to block on the slow server.
	time.Sleep(50 * time.Millisecond)

	closeErr := m.Close()
	require.NoError(t, closeErr)
	// Concurrent / repeated Close must converge on the same single
	// shutdown and report success instead of re-running the close.
	require.NoError(t, m.Close())
	close(release)

	wg.Wait()
	for i, err := range errs {
		require.Error(t, err, "waiter %d got a reply instead of close", i)
	}
	for i, c := range conns {
		// No zero-value replies: every further receive keeps failing, and the
		// connection reports itself as unusable.
		_, err := c.Receive()
		require.Error(t, err, "waiter %d must keep failing after close", i)
		require.Error(t, c.Err())
		_ = c.Close()
	}
}

// TestMuxPipelineOrdering covers requirement 5's ordering clause: pipelined
// commands are read back in write order, including when another mux connection
// interleaves its own commands, and stats only credit fully-received commands.
func TestMuxPipelineOrdering(t *testing.T) {
	srv := startFakeServer(t, echoHandler)
	defer srv.close()
	m := dialMux(t, srv.addr(), time.Second)
	defer m.Close()

	c1 := m.Get()
	c2 := m.Get()
	// Interleave the two connections' writes; replies must still come back
	// in global send order, and each connection only pops its own ids.
	for i := 0; i < 10; i++ {
		require.NoError(t, c1.Send("ECHO", fmt.Sprintf("a%d", i)))
		require.NoError(t, c2.Send("ECHO", fmt.Sprintf("b%d", i)))
	}
	require.NoError(t, c1.Flush())
	require.NoError(t, c2.Flush())

	// Drain in strict global order: c1 owns the even ids and c2 the odd
	// ones. Alternating the receives this way proves the read head advances
	// one full RESP message at a time and replies never get crossed.
	for i := 0; i < 10; i++ {
		v, err := redis.String(c1.Receive())
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("a%d", i), v)
		v, err = redis.String(c2.Receive())
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("b%d", i), v)
	}
	require.NoError(t, c1.Close())
	require.NoError(t, c2.Close())

	stats := m.Stats()
	require.Equal(t, int64(20), stats.Sent)
	require.Equal(t, int64(20), stats.Success)
}

// TestMuxCancelQueuedBeforeTurn verifies the cancellation of a command that
// has not reached the read head: it is marked cancelled and the connection is
// invalidated, while the command's own context error is returned to its caller.
func TestMuxCancelQueuedBeforeTurn(t *testing.T) {
	release := make(chan struct{})
	srv := startFakeServer(t, blockingHandler(t, release))
	defer srv.close()
	m := dialMux(t, srv.addr(), 5*time.Second)

	slow := m.Get()
	queued := m.Get()

	require.NoError(t, slow.Send("ECHO", "slow"))
	require.NoError(t, slow.Flush())
	slowStarted := make(chan struct{})
	go func() {
		close(slowStarted)
		slow.Receive()
	}()
	<-slowStarted
	time.Sleep(50 * time.Millisecond)

	require.NoError(t, queued.Send("ECHO", "queued"))
	require.NoError(t, queued.Flush())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := queued.(redis.ConnWithContext).ReceiveContext(ctx)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	// Poisoning must make the slow waiter converge as well, with an error.
	close(release)
	require.Eventually(t, func() bool { return m.Stats().Error >= 1 },
		2*time.Second, 5*time.Millisecond)

	stats := m.Stats()
	require.Equal(t, int64(2), stats.Sent)
	require.Equal(t, int64(0), stats.Success)
	require.Equal(t, int64(1), stats.Cancelled)
	require.NoError(t, m.Close())
}

// blockingFlushConn wraps a redis.Conn and blocks the first Flush until
// release is closed. It is used to hold the mux write turn without depending
// on TCP buffer backpressure.
type blockingFlushConn struct {
	redis.Conn
	release <-chan struct{}
	once    sync.Once
}

func (b *blockingFlushConn) Flush() error {
	b.once.Do(func() { <-b.release })
	return b.Conn.Flush()
}

// TestMuxCancelWhileWaitingForWriteTurn verifies requirement 1's queueing
// clause: a command blocked behind another writer is still bound by its
// context. On cancellation it is never written, and the mux stays usable.
func TestMuxCancelWhileWaitingForWriteTurn(t *testing.T) {
	srv := startFakeServer(t, echoHandler)
	defer srv.close()

	netConn, err := net.Dial("tcp", srv.addr())
	require.NoError(t, err)
	release := make(chan struct{})
	base := redis.NewConn(netConn, time.Second, time.Second)
	m := redisx.NewConnMux(&blockingFlushConn{Conn: base, release: release})

	holder := m.Get()
	require.NoError(t, holder.Send("ECHO", "holder"))
	flushStarted := make(chan struct{})
	go func() {
		close(flushStarted)
		holder.Flush()
	}()
	<-flushStarted
	// Give the holder time to enter the blocking Flush while keeping the
	// write turn.
	time.Sleep(50 * time.Millisecond)

	queued := m.Get()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = queued.(redis.ConnWithContext).DoContext(ctx, "ECHO", "queued")
	cancel()
	require.True(t, errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled), "got %v", err)

	// Let the holder finish; its command still completes normally because it
	// was never cancelled.
	close(release)
	v, err := redis.String(holder.Receive())
	require.NoError(t, err)
	require.Equal(t, "holder", v)
	require.NoError(t, holder.Close())
	require.NoError(t, queued.Close())

	// The cancelled queued command never reached the server; exactly one
	// command (holder) was sent and succeeded.
	stats := m.Stats()
	require.Equal(t, int64(1), stats.Sent)
	require.Equal(t, int64(1), stats.Success)
	require.Equal(t, int64(0), stats.Cancelled)
	require.NoError(t, m.Close())
}

// TestMuxConcurrentCancelStress hammers a healthy mux from many goroutines:
// it mixes successful commands with commands whose contexts are already done
// (the pre-write cancellation path, which must never poison the mux). It then
// forces one mid-flight cancellation on a fresh mux and asserts that every
// later command converges with an error rather than receiving a crossed
// reply. This covers the no-misalignment invariant under concurrency.
func TestMuxConcurrentCancelStress(t *testing.T) {
	srv := startFakeServer(t, echoHandler)
	defer srv.close()

	// Phase 1: healthy mux, successful commands interleaved with pre-write
	// cancellations. The mux must stay usable throughout.
	m := dialMux(t, srv.addr(), 5*time.Second)
	const goroutines = 16
	const iterations = 25
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				c := m.Get()
				var (
					v   interface{}
					err error
				)
				if i%2 == 1 {
					// Already-cancelled context: must be refused before the
					// command is written, and must not poison the mux.
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					_, err = c.(redis.ConnWithContext).DoContext(ctx, "ECHO", "dropped")
					require.Error(t, err)
				} else {
					v, err = c.(redis.ConnWithContext).DoContext(context.Background(),
						"ECHO", fmt.Sprintf("g%d-i%d", g, i))
					require.NoError(t, err)
					// A successful reply must always be the exact reply for
					// this request: the core no-misalignment invariant.
					s, _ := redis.String(v, nil)
					require.Equal(t, fmt.Sprintf("g%d-i%d", g, i), s)
				}
				_ = c.Close()
			}
		}(g)
	}
	wg.Wait()

	stats := m.Stats()
	// Every written command lands in exactly one terminal bucket and, on a
	// healthy mux, all of them succeed. Commands cancelled before writing
	// appear nowhere: they must not poison the mux or inflate any counter.
	require.Equal(t, stats.Success+stats.Cancelled+stats.Error, stats.Sent)
	require.Equal(t, stats.Success, stats.Sent)
	require.Equal(t, int64(0), stats.Cancelled)
	require.Equal(t, int64(0), stats.Error)
	require.GreaterOrEqual(t, stats.Success, int64(goroutines*iterations/2))
	require.NoError(t, m.Close())

	// Phase 2: force one mid-flight cancellation and verify convergence.
	release := make(chan struct{})
	srv2 := startFakeServer(t, blockingHandler(t, release))
	defer srv2.close()
	m2 := dialMux(t, srv2.addr(), 5*time.Second)

	slow := m2.Get()
	require.NoError(t, slow.Send("ECHO", "slow"))
	require.NoError(t, slow.Flush())
	slowStarted := make(chan struct{})
	go func() {
		close(slowStarted)
		slow.Receive()
	}()
	<-slowStarted
	time.Sleep(50 * time.Millisecond)

	queued := m2.Get()
	require.NoError(t, queued.Send("ECHO", "queued"))
	require.NoError(t, queued.Flush())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, e := queued.(redis.ConnWithContext).ReceiveContext(ctx)
		done <- e
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	// After poisoning, a burst of concurrent commands must all fail, and none
	// may receive another command's reply.
	const burst = 12
	var wg2 sync.WaitGroup
	wg2.Add(burst)
	for i := 0; i < burst; i++ {
		go func() {
			defer wg2.Done()
			c := m2.Get()
			_, err := c.(redis.ConnWithContext).DoContext(context.Background(), "ECHO", "late")
			require.Error(t, err)
			_ = c.Close()
		}()
	}
	close(release)
	wg2.Wait()
	require.NoError(t, m2.Close())
}
