package main

import (
	"flag"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/timothygk/stamper/internal/stamper"
	"github.com/timothygk/stamper/internal/stamper/client"
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
	addrs := flag.String("addrs", "0.0.0.0:8080,0.0.0.0:8081,0.0.0.0:8082", "server address")
	clientId := flag.Int64("client-id", -1, "client id")
	flag.Parse()

	serverAddrs := strings.Split(*addrs, ",")
	client := client.NewClient(
		client.ClientConfig{
			ServerAddrs:   serverAddrs,
			ClientId:      uint64(*clientId),
			RetryDuration: 10000 * time.Millisecond,
		},
		timepkg.NewTime(),
		stamper.JsonEncoderDecoder{},
		CreateTcpConnection,
	)

	for {
		var msg string
		fmt.Scanf("%s", &msg)

		resp, err := client.Request([]byte(msg))
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}
		fmt.Printf("Output: %s\n", string(resp))
	}

}
