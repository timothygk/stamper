package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/timothygk/stamper/internal/assert"
	"github.com/timothygk/stamper/internal/stamper"
	"github.com/timothygk/stamper/internal/timepkg"
)

// CreateTcpConnection create a tcp connection
func CreateTcpConnection(addr string) (net.Conn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	return conn, err
}

func main() {
	servers := flag.String("servers", "0.0.0.0:8080,0.0.0.0:8081,0.0.0.0:8082", "server address")
	port := flag.Int("port", 8080, "port")
	flag.Parse()

	serverAddr := fmt.Sprintf("0.0.0.0:%d", *port)

	addrs := strings.Split(*servers, ",")
	assert.Assert(len(addrs) >= 3, "Should not have 2 or less addrs")

	// compute node id
	nodeId := -1
	for i, addr := range addrs {
		if addr == serverAddr {
			nodeId = i
			break
		}
	}
	assert.Assert(nodeId >= 0 && nodeId < len(addrs), "Node id should be one of the addrs")

	// init replica
	now := time.Now()
	r := rand.New(rand.NewPCG(uint64(now.UnixNano()), uint64(now.Unix())))
	config := stamper.ReplicaConfig{
		SendRetryDuration:       3 * time.Second,
		CommitDelayDuration:     5 * time.Second,
		ViewChangeDelayDuration: 10 * time.Second,
		RecoveryRetryDuration:   10 * time.Second,
		NodeId:                  nodeId,
		ServerAddrs:             addrs,
	}
	replica := stamper.NewReplica(
		config,
		timepkg.NewTime(),
		stamper.JsonEncoderDecoder{},
		CreateTcpConnection,
		r,
		func(body []byte) []byte { return append(body, []byte("_SUFFIXED")...) },
		nil,
		false,
	)
	defer func() {
		fmt.Printf("Final state status:%d, viewId:%d, commitId:%d, lastLogId:%d\n",
			replica.Status(),
			replica.ViewId(),
			replica.CommitId(),
			replica.LastLogId(),
			// TODO: add log hash
		)
	}()

	// init tcp server
	listener, err := net.Listen("tcp", serverAddr)
	if err != nil {
		panic(err)
	}
	closing := make(chan struct{}, 1)
	go func() {
		for {
			select {
			case <-closing:
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					// TODO: log this
					continue
				}
				replica.Accept(conn)
			}
		}
	}()

	// graceful shutdown
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	<-signalCh
	close(closing)
	if err := listener.Close(); err != nil {
		panic(err)
	}
	if err := replica.Close(); err != nil {
		panic(err)
	}

}
