package stamper_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/timothygk/stamper/internal/assert"
	"github.com/timothygk/stamper/internal/logging"
	"github.com/timothygk/stamper/internal/stamper"
	"github.com/timothygk/stamper/internal/stamper/client"
	"github.com/timothygk/stamper/internal/timepkg"
)

func TestSimulation(t *testing.T) {
	synctest.Test(t, simulate)
}

func initReplica(addrs []string, nodeId int, r *rand.Rand, createConn func(string) (net.Conn, error)) *stamper.Replica {
	config := stamper.ReplicaConfig{
		SendRetryDuration:   3 * time.Second,
		CommitDelayDuration: 5 * time.Second,
		NodeId:              nodeId,
		ServerAddrs:         addrs,
	}
	replica := stamper.NewReplica(
		config,
		timepkg.NewTime(),
		stamper.JsonEncoderDecoder{},
		createConn,
		r,
		func(body []byte) []byte { return append(body, []byte("_SUFFIXED")...) },
	)
	return replica
}

func initClient(serverAddrs []string, clientId uint64, createConn func(string) (net.Conn, error)) *client.Client {
	return client.NewClient(
		client.ClientConfig{
			ServerAddrs:   serverAddrs,
			ClientId:      clientId,
			RetryDuration: 5 * time.Second,
		},
		timepkg.NewTime(),
		stamper.JsonEncoderDecoder{},
		createConn,
	)

}

type conn struct {
	net.Conn
	queue  [][]byte
	closed bool
}

func newConn(pipeConn net.Conn) *conn {
	return &conn{
		Conn:   pipeConn,
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
	serverAddrs []string
	replicas    []*stamper.Replica
	clientAddrs []string
	clients     []*client.Client
	conns       []*conn
}

func (n *network) propagate() {
	type ToSend struct {
		conn *conn
		cnt  int
	}

	tosend := []ToSend{}
	for _, conn := range n.conns {
		if len(conn.queue) == 0 {
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
	return func(dstAddr string) (net.Conn, error) {
		dstReplicaIdx := -1
		for i, addr := range n.serverAddrs {
			if addr == dstAddr {
				dstReplicaIdx = i
				break
			}
		}

		assert.Assertf(dstReplicaIdx >= 0, "Unknown dstAddr:%s from srcAddr:%s", dstAddr, srcAddr)

		src, dst := net.Pipe()
		cconn := newConn(src)
		sconn := newConn(dst)
		n.replicas[dstReplicaIdx].Accept(sconn)
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
	const numClients = 10
	const numTicks = 1000
	const requestPerTick = 25
	r := rand.New(rand.NewPCG(123, 456))
	clientR := rand.New(rand.NewPCG(r.Uint64(), r.Uint64()))

	logging.Logf("Start simulation at %v\n", time.Now())

	// init
	n := network{
		r:           rand.New(rand.NewPCG(r.Uint64(), r.Uint64())),
		serverAddrs: iotaWithPrefix("server", numServers, 0),
		clientAddrs: iotaWithPrefix("client", numClients, 0),
	}
	n.replicas = make([]*stamper.Replica, len(n.serverAddrs))
	for i := range n.replicas {
		replicaR := rand.New(rand.NewPCG(r.Uint64(), r.Uint64()))
		n.replicas[i] = initReplica(n.serverAddrs, i, replicaR, n.createConnFunc(n.serverAddrs[i]))
		synctest.Wait()
	}
	n.clients = make([]*client.Client, len(n.clientAddrs))
	for i := range n.clients {
		n.clients[i] = initClient(n.serverAddrs, uint64(i), n.createConnFunc(n.clientAddrs[i]))
		synctest.Wait()
	}

	// main loop
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
		n.propagate()                     // propagate network
		time.Sleep(10 * time.Millisecond) // move time by 10ms
	}

	// extra loops to propagate background timers..
	for range 1000 {
		n.propagate()                      // propagate network
		time.Sleep(300 * time.Millisecond) // move time by 300ms
	}

	for i, r := range n.replicas {
		t.Logf("Replica %d, state: %s", i, r.GetState())
	}

	// close resources
	for i, c := range n.clients {
		assert.Assertf(c.Close() == nil, "Should successfully close client %d", i)
		synctest.Wait()
	}
	for i, r := range n.replicas {
		assert.Assertf(r.Close() == nil, "Should successfully close replica %d", i)
		synctest.Wait()
	}

	logging.Logf("Completed simulation at %v\n", time.Now())
}
