package stamper_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"reflect"
	"runtime"
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
	synctest.Test(t, func(t *testing.T) {
		simulate(t, &SimulatorConfig{
			NumServers: 3,
			NumClients: 10,
			ReplicaConfig: stamper.ReplicaConfig{
				SendRetryDuration:       3 * time.Second,
				CommitDelayDuration:     5 * time.Second,
				ViewChangeDelayDuration: 10 * time.Second,
				RecoveryRetryDuration:   10 * time.Second,
			},
			Seed1:                123123582,
			Seed2:                45679445584,
			RequestPerTick:       2,
			NumTicks:             1000000,
			TickStep:             500 * time.Microsecond,
			TransportDelayMean:   500 * time.Microsecond,
			TransportDelayStdDev: 500 * time.Microsecond,
			MsgLossProb:          Fraction{1, 1000},
			CutOffProb:           Fraction{1, 1000},
			CutOffMean:           10 * time.Second,
			CutOffStdDev:         5 * time.Second,
			RepairProb:           Fraction{5, 100},
		})
	})
}

type Fraction struct {
	Numerator   uint64
	Denominator uint64
}

func flipCoin(r *rand.Rand, prob Fraction) bool {
	return r.Uint64N(prob.Denominator) < prob.Numerator
}

func logNormDuration(r *rand.Rand, mean, stddev, norm time.Duration) time.Duration {
	value := (r.NormFloat64()*float64(stddev) + float64(mean)) / float64(norm)
	return time.Duration(math.Floor(value)) * norm
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

type mockedTicker struct {
	ch      chan time.Time
	failCnt int
}

type mockedTime struct {
	r       *rand.Rand
	now     time.Time
	timers  []*mockedTimer
	tickers []*mockedTicker
}

func (t *mockedTime) Now() time.Time {
	return t.now
}

func (t *mockedTime) AfterFunc(d time.Duration, f func()) timepkg.Timer {
	return t.makeTimer(d, f)
}

func (t *mockedTime) Tick(d time.Duration) <-chan time.Time {
	ticker := &mockedTicker{
		ch:      make(chan time.Time),
		failCnt: 0,
	}
	var f func()
	f = func() {
		// ch <- t.now
		if reflect.ValueOf(ticker.ch).TrySend(reflect.ValueOf(t.now)) {
			ticker.failCnt = 0
		} else {
			ticker.failCnt++
		}
		if ticker.failCnt <= 10 {
			t.makeTimer(d, f)
		}
	}
	t.makeTimer(d, f)
	t.tickers = append(t.tickers, ticker)
	return ticker.ch
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

	// clean up timers
	lastIndex := len(t.timers) - 1
	for i := lastIndex; i >= 0; i-- {
		if t.timers[i].invoked || t.timers[i].stopped {
			// swap to the back
			t.timers[i] = t.timers[lastIndex]
			t.timers[lastIndex] = nil // remove reference
			lastIndex--
		}
	}
	t.timers = t.timers[:lastIndex+1]

	// cleanup tickers
	lastIndex = len(t.tickers) - 1
	for i := lastIndex; i >= 0; i-- {
		if t.tickers[i].failCnt > 10 {
			// heuristic here
			close(t.tickers[i].ch)
			t.tickers[i] = t.tickers[lastIndex]
			t.tickers[lastIndex] = nil // remove reference
			lastIndex--
		}
	}
	t.tickers = t.tickers[:lastIndex+1]
}

func (t *mockedTime) close() error {
	for _, ticker := range t.tickers {
		close(ticker.ch)
	}
	return nil
}

type payload struct {
	data      []byte
	deliverAt time.Time
}

type conn struct {
	net.Conn
	n          *network
	srcIdx     int
	dstIdx     int
	toSchedule [][]byte
	closed     bool
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
	c.toSchedule = append(c.toSchedule, bytes.Clone(b))
	return len(b), nil
}

func (c *conn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	return n, err
}

func (c *conn) Close() error {
	c.closed = true
	return c.Conn.Close()
}

type SimulatorConfig struct {
	NumServers    int
	NumClients    int
	ReplicaConfig stamper.ReplicaConfig
	// rng
	Seed1 uint64
	Seed2 uint64
	// simulation
	RequestPerTick int
	NumTicks       int
	TickStep       time.Duration
	// transport
	TransportDelayMean   time.Duration
	TransportDelayStdDev time.Duration
	// network loss
	MsgLossProb Fraction
	// network partition
	CutOffProb   Fraction
	CutOffMean   time.Duration
	CutOffStdDev time.Duration
	// repair
	RepairProb Fraction
}

type network struct {
	config       *SimulatorConfig
	r            *rand.Rand
	tt           *mockedTime
	serverAddrs  []string
	replicas     []*stamper.Replica
	clientAddrs  []string
	clients      []*client.Client
	conns        []*conn
	serverCutOff []time.Time
}

func newNetwork(config *SimulatorConfig, r *rand.Rand, tt *mockedTime) *network {
	n := network{
		config:       config,
		r:            rand.New(rand.NewPCG(r.Uint64(), r.Uint64())),
		tt:           tt,
		serverAddrs:  iotaWithPrefix("server", config.NumServers, 0),
		clientAddrs:  iotaWithPrefix("client", config.NumClients, 0),
		serverCutOff: make([]time.Time, config.NumServers),
	}
	n.replicas = make([]*stamper.Replica, len(n.serverAddrs))
	for i := range n.replicas {
		n.replicas[i] = n.createReplica(i, false)
		synctest.Wait()
	}
	n.clients = make([]*client.Client, len(n.clientAddrs))
	for i := range n.clients {
		n.clients[i] = initClient(n.serverAddrs, uint64(i), tt, n.createConnFunc(n.clientAddrs[i]))
		synctest.Wait()
	}
	return &n
}

func (n *network) isPartitioned(idx int) bool {
	if idx >= len(n.serverAddrs) {
		return false
	}
	return n.serverCutOff[idx].After(n.tt.now)
}

func (n *network) partition() {
	numPartitioned := 0
	for i := range n.serverCutOff {
		if n.isPartitioned(i) {
			numPartitioned++
		}
		if n.replicas[i].Status() == stamper.ReplicaStatusRecovering {
			numPartitioned++
		}
	}
	if numPartitioned*2+1 < len(n.serverAddrs) && flipCoin(n.r, n.config.CutOffProb) {
		serverId := n.r.IntN(len(n.serverAddrs))
		dur := logNormDuration(n.r, n.config.CutOffMean, n.config.CutOffStdDev, time.Second)
		n.serverCutOff[serverId] = n.tt.now.Add(dur)
		numPartitioned++
	}

	assert.Assertf(numPartitioned*2+1 <= len(n.serverAddrs), "Partition constrain breached, num:%d total:%d", numPartitioned, len(n.serverAddrs))

	// trigger repair
	for i := range n.replicas {
		if !n.isPartitioned(i) && n.serverCutOff[i].After(n.tt.now.Add(-n.config.TickStep)) && flipCoin(n.r, n.config.RepairProb) {
			// trigger repair
			assert.Assertf(n.replicas[i].Close() == nil, "Should be able to close replica %d", i)
			n.replicas[i] = n.createReplica(i, true)
			synctest.Wait()
		}
	}
}

func (n *network) propagate(finishing bool) {
	// schedule queued payloads with random delay & out-of-order delivery
	for _, conn := range n.conns {
		for _, data := range conn.toSchedule {
			dur := n.config.TransportDelayMean
			if !finishing {
				dur = logNormDuration(n.r, n.config.TransportDelayMean, n.config.TransportDelayStdDev, time.Microsecond)
			}

			n.tt.makeTimer(dur, func() {
				if n.isPartitioned(conn.dstIdx) {
					conn.Close()
					synctest.Wait()
				}
				if !finishing && flipCoin(n.r, n.config.MsgLossProb) {
					return // msg loss
				}
				// deliver
				conn.Conn.Write(data)
				synctest.Wait()
			})
		}
		conn.toSchedule = nil
	}

	// cleanup
	for i := len(n.conns) - 1; i >= 0; i-- {
		conn := n.conns[i]
		if conn.closed {
			lastIdx := len(n.conns) - 1
			if i < lastIdx {
				n.conns[i] = n.conns[lastIdx]
				n.conns[lastIdx] = nil
			}
			n.conns = n.conns[:lastIdx]
			continue
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
		return cconn, nil
	}
}

func (n *network) createReplica(i int, repair bool) *stamper.Replica {
	replicaConfig := n.config.ReplicaConfig // copy
	replicaConfig.NodeId = i
	replicaConfig.ServerAddrs = n.serverAddrs
	return stamper.NewReplica(
		replicaConfig,
		n.tt,
		stamper.JsonEncoderDecoder{},
		n.createConnFunc(n.serverAddrs[i]),
		rand.New(rand.NewPCG(n.r.Uint64(), n.r.Uint64())),
		func(body []byte) []byte { return append(body, []byte("_SUFFIXED")...) },
		repair,
	)
}

func iotaWithPrefix(prefix string, num int, start int) []string {
	result := make([]string, 0, num)
	for i := range num {
		result = append(result, fmt.Sprintf("%s-%d", prefix, start+i))
	}
	return result
}

func simulate(t *testing.T, config *SimulatorConfig) {
	// init
	r := rand.New(rand.NewPCG(config.Seed1, config.Seed2))
	tt := mockedTime{
		r:   r,
		now: time.Now(), // deterministic
	}
	n := newNetwork(config, r, &tt)

	// main loop
	t.Logf("Start simulation at %v\n", tt.now)
	tickCnt := 0
	for range config.NumTicks {
		// generate requests
		for range config.RequestPerTick {
			index := r.IntN(len(n.clients))
			payload := make([]byte, 8)
			binary.LittleEndian.PutUint64(payload, r.Uint64())
			go func() {
				result, err := n.clients[index].Request(payload)
				assert.Assertf(err == nil || err == client.ErrAnotherRequestInflight || err == client.ErrClientClosed, "Unknown error: %v", err)
				if err == nil {
					assert.Assertf(
						bytes.Equal(result, append(payload, []byte("_SUFFIXED")...)),
						"Expected prefix: %x, actual: %x",
						payload,
						result,
					)
				}
			}()
			synctest.Wait()
		}
		n.partition()                   // network partition
		n.propagate(false)              // propagate network messages
		tt.advanceTime(config.TickStep) // advance time

		if tickCnt%100000 == 0 {
			t.Logf(
				"At tick %d, timers:%d timerchans:%d managedconn:%d numGoroutines=%d",
				tickCnt,
				len(tt.timers),
				len(tt.tickers),
				len(n.conns),
				runtime.NumGoroutine(),
			)
		}
		tickCnt++
	}

	for i, r := range n.replicas {
		t.Logf("Replica %d, state: %s", i, r.GetState())
	}
	t.Logf("Cleanup loop at %v...", tt.now)

	// extra loops to propagate background timers..
	for d := 5 * time.Minute; d >= 0; d -= config.TickStep {
		n.propagate(true)               // propagate network
		tt.advanceTime(config.TickStep) // advance time
	}

	states := []string{}
	for i, r := range n.replicas {
		states = append(states, r.GetState())
		t.Logf("Replica %d, state: %s", i, states[i])
	}
	for i := range len(states) - 1 {
		assert.Assertf(
			states[i] == states[i+1],
			"State of replica %d and %d is not equal\n\t%s\n\t\tvs\n\t%s",
			i,
			i+1,
			states[i],
			states[i+1],
		)
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
