package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"regexp"
	"sync/atomic"
	"syscall"
	"time"
)

func fixSocketLimit() error {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return fmt.Errorf("Error Getting Rlimit: %s", err)
	}
	fmt.Println(rLimit)
	rLimit.Max = 8192 //4096
	rLimit.Cur = 8192 //4096
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return fmt.Errorf("Error Setting Rlimit: %s", err)
	}
	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return fmt.Errorf("Error Getting Rlimit: %s", err)
	}
	fmt.Println("Rlimit Final", rLimit)

	return nil
}

type subscriber struct {
	*regexp.Regexp
	nodeClient *nodeClient
}

var subs = []subscriber{}

var counter uint64

func main() {
	fixSocketLimit()

	address, err := net.ResolveTCPAddr("tcp4", ":5987")

	if err != nil {
		fmt.Println("error resolving tcp4 address", err)
		return
	}

	listener, err := net.ListenTCP("tcp4", address)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer listener.Close()

	go func() {
		for {
			fmt.Printf("%v messages per second\n", atomic.LoadUint64(&counter))
			atomic.StoreUint64(&counter, 0)
			time.Sleep(time.Second)
		}
	}()

	fmt.Println("Listening on", listener.Addr().String())
	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			fmt.Println(err)
			break
		}

		go handleClient(&nodeClient{conn})
	}

	fmt.Println("Done.")
}

func handlePublish(topic []byte, payload []byte) {
	for _, sub := range subs {
		if sub.Match(topic) {
			atomic.AddUint64(&counter, 1)
			go sub.nodeClient.publish(topic, payload)
		}
	}
}

func whelele(conn io.Reader, into *[]byte) error {
	var sure int

	intocap := cap(*into)

	for {
		tmp := make([]byte, intocap-sure)

		// read topic
		n, err := conn.Read(tmp)

		// make sure no error occurred
		if err != nil {
			return err
		}

		sure += n

		*into = append(*into, tmp...)

		if sure == intocap {
			break
		}
	}

	return nil
}

func handleClient(client *nodeClient) {
	fmt.Println("Client connected", client.RemoteAddr().String())

	defer func() {
		client.Close()
		fmt.Println("Client gone", client.RemoteAddr().String())
	}()

	reader := bufio.NewReader(client)
	for {
		t, err := reader.ReadByte()

		if err != nil {
			fmt.Println("error reading type byte", err)
			return
		}

		switch t {
		case PUBLISH:
			var topiclen uint32
			binary.Read(reader, binary.LittleEndian, &topiclen)

			topic := make([]byte, topiclen)
			err = whelele(reader, &topic)

			if err != nil {
				fmt.Println("error reading topic", err)
				return
			}

			var payloadlen uint32
			binary.Read(reader, binary.LittleEndian, &payloadlen)

			payload := make([]byte, payloadlen)
			err = whelele(reader, &payload)

			if err != nil {
				fmt.Print("error reading payload", err)
			}

			go handlePublish(topic, payload)

			break
		case SUBSCRIBE:
			var topiclen uint32
			binary.Read(reader, binary.LittleEndian, &topiclen)

			topic := make([]byte, topiclen)
			n, err := reader.Read(topic)

			if err != nil {
				fmt.Println("error reading topic", err)
				return
			}

			if n != len(topic) {
				fmt.Println("could not read whole topic; wanted", cap(topic), "only got", n)
				return
			}

			regex, err := regexp.Compile(string(topic) + ".*")

			if err != nil {
				fmt.Println("failed to compile regex", err)
				return
			}

			subs = append(subs, subscriber{regex, client})

			break
		default:
			fmt.Println("unknown message type")
			break
		}
	}
}
