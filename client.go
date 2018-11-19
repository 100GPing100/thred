package main

import (
	"bytes"
	"encoding/binary"
	"net"
)

/*
-- Subscribe {
	Type     byte   // = 1
	TopicLen uint32
	Topic    []byte // len(Topic) == TopicLen
}

-- Unsubscribe {
	Type     byte   // = 2
	TopicLen uint32
	Topic    []byte // len(Topic) == TopicLen
}

-- Publish {
	Type       byte   // = 3
	TopicLen   uint32
	Topic      []byte // len(Topic) == TopicLen
	PayloadLen uint32
	Payload    byte[] // len(Payload) == PayloadLen
}

*/

// Message types.
const (
	SUBSCRIBE   = byte(1)
	UNSUBSCRIBE = byte(2)
	PUBLISH     = byte(3)
)

type nodeClient struct {
	net.Conn
}

func (c *nodeClient) publish(topic []byte, payload []byte) {
	topiclen := uint32(len(topic))
	payloadlen := uint32(len(payload))

	buff := bytes.NewBuffer(make([]byte, 0, 1+4+4+topiclen+payloadlen))

	buff.WriteByte(PUBLISH)
	binary.Write(buff, binary.LittleEndian, topiclen)
	buff.Write(topic)
	binary.Write(buff, binary.LittleEndian, payloadlen)
	buff.Write(payload)

	buff.WriteTo(c)
}
