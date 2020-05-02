package nsqd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"time"
)

const (
	MsgIDLength       = 16
	minValidMsgLength = MsgIDLength + 8 + 2 // Timestamp + Attempts
)

type MessageID [MsgIDLength]byte

type Message struct {
	ID         MessageID
	Body       []byte
	SeqID      uint64
	Addr       net.IP
	Timestamp  int64
	IdealTime  int64
	Ktimestamp time.Time
	Attempts   uint16
	lastSendTS time.Time

	NSQDAddress string

	// for in-flight handling
	deliveryTS time.Time
	clientID   int64
	pri        int64
	index      int
	deferred   time.Duration

	//RTM.dtb
	senderID int64
	sendTime int64
	pubClient *clientV2
}

func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

// NewNullMessage creates a null message
func NewNullMessage(seqID uint64) *Message {
	return &Message{
		SeqID:     seqID,
		Timestamp: time.Now().UnixNano(),
		Body:      []byte("NULL"),
	}
}

func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [18]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))
	binary.BigEndian.PutUint64(buf[10:18], m.SeqID)

	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.ID[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

func (m *Message) WriteToV2(w io.Writer, buf []byte) (int64, error) {
        var total int64

        binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
        binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))
        binary.BigEndian.PutUint64(buf[10:18], m.SeqID)

        n, err := w.Write(buf[:])
        total += int64(n)
        if err != nil {
                return total, err
        }

        n, err = w.Write(m.ID[:])
        total += int64(n)
        if err != nil {
                return total, err
        }

        n, err = w.Write(m.Body)
        total += int64(n)
        if err != nil {
                return total, err
        }
        return total, nil
}


func decodeMessage(b []byte) (*Message, error) {
	var msg Message

	if len(b) < minValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	msg.SeqID = binary.BigEndian.Uint64(b[10:18])

	buf := bytes.NewBuffer(b[18:])

	_, err := io.ReadFull(buf, msg.ID[:])
	if err != nil {
		return nil, err
	}

	msg.Body, err = ioutil.ReadAll(buf)
	if err != nil {
		return nil, err
	}

	return &msg, nil
}

func writeMessageToBackend(buf *bytes.Buffer, msg *Message, bq BackendQueue) error {
	buf.Reset()
	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}
	return bq.Put(buf.Bytes())
}
