package stamper_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"reflect"
	"sort"
	"testing"
	"testing/synctest"
	"time"

	"github.com/timothygk/stamper/internal/assert"
	"github.com/timothygk/stamper/internal/stamper"
	"github.com/timothygk/stamper/internal/stamper/client"
	"github.com/timothygk/stamper/internal/timepkg"
)

func TestSimulation(t *testing.T) {
	synctest.Test(t, simulate)
}

type Fraction struct {
	Numerator   uint64
	Denominator uint64
}

func flipCoin(r *rand.Rand, prob Fraction) bool {
	return r.Uint64N(prob.Denominator) < prob.Numerator
}

func initReplica(addrs []string, nodeId int, r *rand.Rand, tt timepkg.Time, createConn func(string) (net.Conn, error)) *stamper.Replica {
	config := stamper.ReplicaConfig{
		SendRetryDuration:       3 * time.Second,
		CommitDelayDuration:     5 * time.Second,
		ViewChangeDelayDuration: 10 * time.Second,
		NodeId:                  nodeId,
		ServerAddrs:             addrs,
	}
	replica := stamper.NewReplica(
		config,
		tt,
		stamper.JsonEncoderDecoder{},
		createConn,
		r,
		func(body []byte) []byte { return append(body, []byte("_SUFFIXED")...) },
	)
	return replica
}

func initClient(serverAddrs []string, clientId uint64, tt timepkg.Time, createConn func(string) (net.Conn, error)) *client.Client {
	return client.NewClient(
		client.ClientConfig{
			ServerAddrs:   serverAddrs,
			ClientId:      clientId,
			RetryDuration: 5 * time.Second,
		},
		tt,
		stamper.JsonEncoderDecoder{},
		createConn,
	)
}

type mockedTimer struct {
	f        func()
	id       uint64
	duration time.Duration
	stopped  bool
	invoked  bool
}

func (t *mockedTimer) Stop() bool {
	if !t.invoked {
		t.stopped = true
		return true
	}
	return false
}

func (t *mockedTimer) Reset(d time.Duration) bool {
	t.duration = d
	isDone := t.invoked || t.stopped
	t.stopped = false
	t.invoked = false
	return !isDone
}

type mockedTime struct {
	r      *rand.Rand
	now    time.Time
	timers []*mockedTimer
	chans  []chan time.Time
}

func (t *mockedTime) Now() time.Time {
	return t.now
}

func (t *mockedTime) AfterFunc(d time.Duration, f func()) timepkg.Timer {
	return t.makeTimer(d, f)
}

func (t *mockedTime) Tick(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time)
	var f func()
	f = func() {
		// ch <- t.now
		reflect.ValueOf(ch).TrySend(reflect.ValueOf(t.now))
		t.makeTimer(d, f)
	}
	t.makeTimer(d, f)
	t.chans = append(t.chans, ch)
	return ch
}

func (t *mockedTime) makeTimer(d time.Duration, fn func()) *mockedTimer {
	timer := &mockedTimer{
		id:       t.r.Uint64(), // generate random id
		duration: d,
		f:        fn,
		stopped:  false,
		invoked:  false,
	}
	t.timers = append(t.timers, timer)
	return timer
}

func (t *mockedTime) advanceTime(d time.Duration) {
	sort.Slice(t.timers, func(i, j int) bool {
		// order by (duration, id)
		return t.timers[i].duration < t.timers[j].duration ||
			t.timers[i].duration == t.timers[j].duration && t.timers[i].id < t.timers[j].id
	})

	t.now = t.now.Add(d)
	for _, timer := range t.timers {
		timer.duration -= d
		if timer.duration <= 0 && !timer.stopped {
			// invoke
			timer.invoked = true
			timer.f()
			synctest.Wait()
		}
	}

	// clean up slice
	lastIndex := len(t.timers) - 1
	for i := lastIndex; i >= 0; i-- {
		if t.timers[i].invoked || t.timers[i].stopped {
			// swap to the back
			t.timers[i], t.timers[lastIndex] = t.timers[lastIndex], t.timers[i]
			t.timers[lastIndex] = nil // remove reference
			lastIndex--
		}
	}
	t.timers = t.timers[:lastIndex+1]
}

func (t *mockedTime) close() error {
	for _, ch := range t.chans {
		// reflect.ValueOf(ch).TryRecv()
		close(ch)
	}
	return nil
}

type conn struct {
	net.Conn
	srcIdx int
	dstIdx int
	queue  [][]byte
	closed bool
}

func newConn(pipeConn net.Conn, srcIdx, dstIdx int) *conn {
	return &conn{
		Conn:   pipeConn,
		srcIdx: srcIdx,
		dstIdx: dstIdx,
		closed: false,
	}
}

func (c *conn) Write(b []byte) (int, error) {
	if c.closed {
		return 0, io.EOF
	}
	// logging.Logf("Write to conn %p c.Conn %p ch %p, %s\n", c, c.Conn, c.queue, string(b))
	c.queue = append(c.queue, bytes.Clone(b))
	return len(b), nil
}

func (c *conn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	// logging.Logf("Read recv conn %p c.Conn %p %s\n", c, c.Conn, string(b[:n]))
	return n, err
}

func (c *conn) Close() error {
	c.closed = true
	return c.Conn.Close()
}

func (c *conn) deliverOne() {
	payload := c.queue[0]
	c.queue = c.queue[1:]
	// logging.Logf("deliverOne to conn %p c.Conn %p ch %p, %s\n", c, c.Conn, c.queue, string(payload))
	c.Conn.Write(payload)
}

type network struct {
	r           *rand.Rand
	now         func() time.Time
	serverAddrs []string
	replicas    []*stamper.Replica
	clientAddrs []string
	clients     []*client.Client
	conns       []*conn
	// network partition
	serverCutOff []time.Time
	cutOffProb   Fraction
	cutOffMin    time.Duration
	cutOffMax    time.Duration
}

func (n *network) isPartitioned(idx int) bool {
	if idx >= len(n.serverAddrs) {
		return false
	}
	return n.serverCutOff[idx].After(n.now())
}

func (n *network) partition() {
	numPartitioned := 0
	for i := range n.serverCutOff {
		if n.isPartitioned(i) {
			numPartitioned++
		}
	}
	if numPartitioned*2+1 < len(n.serverAddrs) && flipCoin(n.r, n.cutOffProb) {
		serverId := n.r.IntN(len(n.serverAddrs))
		dur := time.Duration(n.r.Int64N(int64(n.cutOffMax-n.cutOffMin))) + n.cutOffMin
		n.serverCutOff[serverId] = n.now().Add(dur)
		numPartitioned++
	}

	assert.Assertf(numPartitioned*2+1 <= len(n.serverAddrs), "Partition constrain breached, num:%d total:%d", numPartitioned, len(n.serverAddrs))
}

func (n *network) propagate() {
	type ToSend struct {
		conn *conn
		cnt  int
	}

	tosend := []ToSend{}
	for _, conn := range n.conns {
		if conn.closed {
			continue
		}
		if len(conn.queue) == 0 {
			continue
		}
		if n.isPartitioned(conn.dstIdx) {
			conn.Close()
			synctest.Wait()
			continue
		}
		numToSend := n.r.IntN(len(conn.queue) + 1)
		if numToSend > 0 {
			tosend = append(tosend, ToSend{conn, numToSend})
		}
	}

	// shuffled delivery
	for len(tosend) > 0 {
		// pick one to send the network request
		i := n.r.IntN(len(tosend))
		tosend[i].conn.deliverOne()
		synctest.Wait()
		// decr counter & cleanup if needed
		tosend[i].cnt--
		if tosend[i].cnt == 0 {
			lastIdx := len(tosend) - 1
			if i != lastIdx {
				tosend[i], tosend[lastIdx] = tosend[lastIdx], tosend[i]
			}
			tosend = tosend[:lastIdx]
		}
	}
}

func (n *network) createConnFunc(srcAddr string) func(string) (net.Conn, error) {
	srcIdx := -1
	for i, addr := range n.serverAddrs {
		if addr == srcAddr {
			srcIdx = i
			break
		}
	}
	if srcIdx == -1 {
		for i, addr := range n.clientAddrs {
			if addr == srcAddr {
				srcIdx = i + len(n.serverAddrs)
				break
			}
		}
	}
	assert.Assertf(srcIdx >= 0, "Unknown srcAddr:%s", srcAddr)
	return func(dstAddr string) (net.Conn, error) {
		dstIdx := -1
		for i, addr := range n.serverAddrs {
			if addr == dstAddr {
				dstIdx = i
				break
			}
		}

		assert.Assertf(dstIdx >= 0, "Unknown dstAddr:%s from srcAddr:%s", dstAddr, srcAddr)

		if n.isPartitioned(dstIdx) {
			return nil, io.EOF
		}

		src, dst := net.Pipe()
		cconn := newConn(src, srcIdx, dstIdx)
		sconn := newConn(dst, dstIdx, srcIdx)
		n.replicas[dstIdx].Accept(sconn)
		n.conns = append(n.conns, cconn)
		n.conns = append(n.conns, sconn)
		// logging.Logf("Create conn src: (%s, %p, %p, %p) dst: (%s, %p, %p, %p)\n",
		// 	srcAddr, cconn, cconn.Conn, cconn.queue,
		// 	dstAddr, sconn, sconn.Conn, sconn.queue,
		// )
		return cconn, nil
	}
}

func iotaWithPrefix(prefix string, num int, start int) []string {
	result := make([]string, 0, num)
	for i := range num {
		result = append(result, fmt.Sprintf("%s-%d", prefix, start+i))
	}
	return result
}

func simulate(t *testing.T) {
	const numServers = 3
	const numClients = 1
	const numTicks = 3000
	const requestPerTick = 1
	r := rand.New(rand.NewPCG(123, 456))
	clientR := rand.New(rand.NewPCG(r.Uint64(), r.Uint64()))

	// init
	tt := mockedTime{
		r:   rand.New(rand.NewPCG(r.Uint64(), r.Uint64())),
		now: time.Now(), // deterministic
	}
	n := network{
		r:            rand.New(rand.NewPCG(r.Uint64(), r.Uint64())),
		now:          tt.Now,
		serverAddrs:  iotaWithPrefix("server", numServers, 0),
		clientAddrs:  iotaWithPrefix("client", numClients, 0),
		serverCutOff: make([]time.Time, numServers),
		cutOffProb:   Fraction{35, 10000},
		cutOffMin:    time.Second,
		cutOffMax:    15 * time.Second,
	}
	n.replicas = make([]*stamper.Replica, len(n.serverAddrs))
	for i := range n.replicas {
		replicaR := rand.New(rand.NewPCG(r.Uint64(), r.Uint64()))
		n.replicas[i] = initReplica(n.serverAddrs, i, replicaR, &tt, n.createConnFunc(n.serverAddrs[i]))
		synctest.Wait()
	}
	n.clients = make([]*client.Client, len(n.clientAddrs))
	for i := range n.clients {
		n.clients[i] = initClient(n.serverAddrs, uint64(i), &tt, n.createConnFunc(n.clientAddrs[i]))
		synctest.Wait()
	}

	// main loop
	t.Logf("Start simulation at %v\n", tt.now)
	for range numTicks {
		// generate requests
		for range requestPerTick {
			index := clientR.IntN(len(n.clients))
			payload := make([]byte, 8)
			binary.LittleEndian.PutUint64(payload, clientR.Uint64())
			go func() {
				result, err := n.clients[index].Request(payload)
				assert.Assertf(err == nil || err == client.ErrAnotherRequestInflight || err == client.ErrClientClosed, "Unknown error: %v", err)
				if err == nil {
					assert.Assertf(bytes.HasPrefix(result, payload), "Expected prefix: %x, actual: %x", payload, result)
				}
			}()
			synctest.Wait()
		}
		n.partition()                          // network partition
		n.propagate()                          // propagate network messages
		tt.advanceTime(100 * time.Millisecond) // advance time
	}

	for i, r := range n.replicas {
		t.Logf("Replica %d, state: %s", i, r.GetState())
	}
	t.Logf("Cleanup loop at %v...", tt.now)

	// extra loops to propagate background timers..
	for range 1000 {
		n.propagate()                          // propagate network
		tt.advanceTime(300 * time.Millisecond) // advance time
	}

	for i, r := range n.replicas {
		t.Logf("Replica %d, state: %s", i, r.GetState())
	}

	// close resources
	tt.close()
	for i, c := range n.clients {
		assert.Assertf(c.Close() == nil, "Should successfully close client %d", i)
		//synctest.Wait()
	}
	for i, r := range n.replicas {
		assert.Assertf(r.Close() == nil, "Should successfully close replica %d", i)
		//synctest.Wait()
	}
	synctest.Wait()
	t.Logf("Completed simulation at %v\n", tt.now)
}
