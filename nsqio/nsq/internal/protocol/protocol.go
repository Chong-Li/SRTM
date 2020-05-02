package protocol

import (
	"encoding/binary"
	"io"
	"net"
)

// Protocol describes the basic behavior of any protocol in the system
type Protocol interface {
	IOLoop(conn net.Conn) error
}

// SendResponse is a server side utility function to prefix data with a length header
// and write to the supplied Writer
func SendResponse(w io.Writer, data []byte) (int, error) {
	err := binary.Write(w, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}

	n, err := w.Write(data)
	if err != nil {
		return 0, err
	}

	return (n + 4), nil
}

func SendAddr(w io.Writer, addr string) (int, error) {
	var beBuf [4]byte
	addrSize := len(addr) + 1
	if addr == "" {
		addrSize = 0
	}
	binary.BigEndian.PutUint32(beBuf[:], uint32(addrSize))
	n, err := w.Write(beBuf[:])
	if err != nil {
		return n, err
	}
	if addrSize == 0 {
		return 4, err
	}
	n, err = w.Write([]byte(addr + "\n"))
	if err != nil {
		return n + 4, err
	}
	return n + 4, err
}

// SendFramedResponse is a server side utility function to prefix data with a length header
// and frame header and write to the supplied Writer
func SendFramedResponse(w io.Writer, frameType int32, data []byte) (int, error) {
	beBuf := make([]byte, 4)
	size := uint32(len(data)) + 4

	binary.BigEndian.PutUint32(beBuf, size)
	n, err := w.Write(beBuf)
	if err != nil {
		return n, err
	}

	binary.BigEndian.PutUint32(beBuf, uint32(frameType))
	n, err = w.Write(beBuf)
	if err != nil {
		return n + 4, err
	}

	n, err = w.Write(data)
	return n + 8, err
}

func SendFramedResponseV2(w io.Writer, frameType int32, data []byte, beBuf []byte) (int, error) {
        size := uint32(len(data)) + 4

        binary.BigEndian.PutUint32(beBuf, size)
        n, err := w.Write(beBuf)
        if err != nil {
                return n, err
        }

        binary.BigEndian.PutUint32(beBuf, uint32(frameType))
        n, err = w.Write(beBuf)
        if err != nil {
                return n + 4, err
        }

        n, err = w.Write(data)
        return n + 8, err
}

