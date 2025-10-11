package stamper_test

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/timothygk/stamper/internal/assert"
	"github.com/timothygk/stamper/internal/stamper"
	"github.com/timothygk/stamper/internal/stamper/client"
	"github.com/timothygk/stamper/internal/timepkg"
)

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

type network struct {
	serverAddrs []string
	replicas    []*stamper.Replica
	clientAddrs []string
	clients     []*client.Client
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
		n.replicas[dstReplicaIdx].Accept(dst)
		return src, nil
	}
}

func simulate(t *testing.T) {
	const numTicks = 100
	const requestPerTick = 2
	r := rand.New(rand.NewPCG(123, 456))
	clientR := rand.New(rand.NewPCG(r.Uint64(), rand.Uint64()))

	// init
	n := network{
		serverAddrs: []string{"server0", "server1", "server2"},
		clientAddrs: []string{"client0", "client1", "client2", "client3"},
	}
	n.replicas = make([]*stamper.Replica, len(n.serverAddrs))
	for i := range n.replicas {
		n.replicas[i] = initReplica(n.serverAddrs, i, r, n.createConnFunc(n.serverAddrs[i]))
	}
	n.clients = make([]*client.Client, len(n.clientAddrs))
	for i := range n.clients {
		n.clients[i] = initClient(n.serverAddrs, uint64(i), n.createConnFunc(n.clientAddrs[i]))
	}
	synctest.Wait()

	// main loop
	for range numTicks {
		for range requestPerTick {
			index := clientR.IntN(len(n.clients))
			payload := make([]byte, 8)
			binary.LittleEndian.PutUint64(payload, clientR.Uint64())
			go func() {
				result, err := n.clients[index].Request(payload)
				assert.Assertf(err == nil || err == client.ErrAnotherRequestInflight, "Unknown error: %v", err)
				if err == nil {
					assert.Assertf(bytes.HasPrefix(result, payload), "Expected prefix: %x, actual: %x", payload, result)
				}
			}()
			synctest.Wait()
		}
	}

	for i, r := range n.replicas {
		t.Logf("Replica %d, state: %s", i, r.GetState())
	}

	// close resources
	for i, c := range n.clients {
		assert.Assertf(c.Close() == nil, "Should successfully close client %d", i)
	}
	for i, r := range n.replicas {
		assert.Assertf(r.Close() == nil, "Should successfully close replica %d", i)
	}
	synctest.Wait()
}

func TestSimulation(t *testing.T) {
	synctest.Test(t, simulate)
}
