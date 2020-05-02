// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bufio implements buffered I/O. It wraps an io.Reader or io.Writer
// object, creating another object (Reader or Writer) that also implements
// the interface but provides buffering and some help for textual I/O.
package nsqd

import (
	"bytes"
	"errors"
	"net"
	"strconv"
	"fmt"
	//"encoding/binary"
	//"unsafe"
)


type TimeStamp struct {
	stamp int64
	last int
}

// Reader implements buffering for an io.Reader object.
type Reader struct {
	buf          []byte
	conn         *net.TCPConn // reader provided by the client
	r, w         int       // buf read and write positions
	times        [1024]TimeStamp
        tr, tw       int
	control      [2048]byte
	err          error
}

const maxConsecutiveEmptyReads = 100
var errNegativeRead = errors.New("bufio: reader returned negative count from Read")

// NewReader returns a new Reader whose buffer has the default size.
func NewReader(c *net.TCPConn, size int) *Reader {
	b := &Reader{
		conn: c,
	}
	b.buf = make([]byte, size)
	//b.times = make([]TimeStamp, 1024)
	//b.control = make([]byte, 1024)
	fmt.Printf("Our TimeReader created\n")
	return b
}

func (b *Reader) fillMsg() {
        // Slide existing data to beginning.
        if b.r > 0 {
                copy(b.buf, b.buf[b.r:b.w])
		move := b.r - 0
		x:=0
		for j:= b.tr; j< b.tw; j++ {
			b.times[x].stamp = b.times[j].stamp
			b.times[x].last = b.times[j].last-move
			x++
		}
                b.w -= b.r
                b.r = 0
		b.tw -= b.tr
		b.tr = 0
        }

        if b.w >= len(b.buf) {
                //panic("bufio: tried to fill full buffer")
		b.err = ErrShortBuffer
		return
        }

        // Read new data: try a limited number of times.
        for i := maxConsecutiveEmptyReads; i > 0; i-- {
		n,cn, _,_,err:=b.conn.ReadMsgTCP(b.buf[b.w:], b.control[:])
		//fmt.Printf("fill.ReadMsg: %v %v\n", n, cn)
                if n < 0 {
                        //panic(errNegativeRead)
			b.err = err
			return
                }
		last := b.w
                b.w += n
		for j:= 16; j < cn; {
			//or use encoding/binary.BigEndian... or ReadVarint
			if x := bytes.IndexByte(b.control[j:cn], '!'); x >= 0 {
				if x < 17 {
					break
				}
				b.times[b.tw].stamp, _ = strconv.ParseInt(string(b.control[j:j+x]), 10, 64)
                                //stest, _ := binary.Varint(b.control[j:j+x])
                               	//stest2 := binary.BigEndian.Uint64(b.control[j:j+x])
                               	//stest3 := binary.LittleEndian.Uint64(b.control[j:j+x])
				//fmt.Printf("stamp=%v %v %v %v\n", b.times[b.tw].stamp, stest, stest2, stest3)
				j=j+x+1
				if x := bytes.IndexByte(b.control[j:cn], '!'); x >= 0 {
					size, _ := strconv.Atoi(string(b.control[j:j+x]))
					//size := int(binary.BigEndian.Uint64(b.control[j:j+x]))
					last += size
					b.times[b.tw].last = last
					//fmt.Printf("last=%v\n", b.times[b.tw].last)
					j=j+x+1
					b.tw = b.tw + 1
				}
                	} else {
				break
			}
		}
		if last!=b.w {
		//if n > 1024 {
			//fmt.Printf("~~~~!!!! fillMsg Error: unmatched last=%v b.w=%v b.tw=%v n=%v cn=%v %v \n", last, b.w, b.tw, n, cn)
			fmt.Printf("~~~~!!!! fillMsg Error %v: unmatched last=%v b.w=%v b.tw=%v n=%v cn=%v %v \n", err, last, b.w, b.tw, n, cn, b.control[:cn])
			b.times[b.tw].last = b.w
			b.times[b.tw].stamp = b.times[b.tw-1].stamp
			b.tw = b.tw + 1
		}

                if err != nil {
			b.err = err
                        return
                }
                if n > 0 {
                        return
                }
        }
}

func (b *Reader) readErr() error {
	err := b.err
	b.err = nil
	return err
}

func (b *Reader) ReadMsg(p []byte) (n int, err error) {
        n = len(p)
        if n == 0 {
                return 0, EOF
        }
        if b.r == b.w {
                if b.err != nil {
                        return 0, b.readErr()
                }
                // One read.
                // Do not use b.fill, which will loop.
                b.r = 0
                b.w = 0
		b.tr = 0
		b.tw = 0
		cn := 0
		n,cn, _,_,err =b.conn.ReadMsgTCP(b.buf[b.w:], b.control[:])
		//fmt.Printf("Read: %v %v\n", n, cn)
                if n < 0 {
                        //panic(errNegativeRead)
			return n, EOF
                }
                if n == 0 {
                        return 0, EOF
                }
                last := b.w
                b.w += n
                for j:= 16; j < cn; {
                        //or use encoding/binary.BigEndian... or ReadVarint
                        if x := bytes.IndexByte(b.control[j:cn], '!'); x >= 0 {
				if x< 17 {
					break
				}
                                b.times[b.tw].stamp, _ = strconv.ParseInt(string(b.control[j:j+x]), 10, 64)
                                //b.times[b.tw].stamp, _ = binary.Varint(b.control[j:j+x])
				//fmt.Printf("stamp=%v ", b.times[b.tw].stamp)
                                j=j+x+1
                                if x := bytes.IndexByte(b.control[j:cn], '!'); x >= 0 {
                                        size, _ := strconv.Atoi(string(b.control[j:j+x]))
					//size := int(binary.BigEndian.Uint64(b.control[j:j+x]))
                                        last += size
                                        b.times[b.tw].last = last
					//fmt.Printf("last=%v\n", b.times[b.tw].last)
                                	j=j+x+1
                                	b.tw = b.tw + 1
                                }
                        } else {
				break
			}
                }
                if last!=b.w {
                        fmt.Printf("~~~~!!!! Error: control and msg unmatched %v %v\n", last, b.w)
			b.times[b.tw].last = b.w
                        b.times[b.tw].stamp = b.times[b.tw-1].stamp
                        b.tw = b.tw + 1
                }
		if err != nil {
			return n, EOF
		}
        }
        // copy as much as we can
        n = copy(p, b.buf[b.r:b.w])
        b.r += n
	if b.r > b.times[b.tr].last {
             for j:= b.tr; j<b.tw; j++ {
                  if b.r <= b.times[j].last {
                        b.tr = j
                        break
                  }
             }
        }
        return n, nil
}

var ErrUnexpectedEOF = errors.New("unexpected EOF")
var ErrShortBuffer = errors.New("shortBuffer")
var EOF = errors.New("EOF")


func (b *Reader) ReadFull(buf []byte) (n int, err error) {
	min:=len(buf)
	for n < min && err == nil {
		var nn int
		nn, err = b.ReadMsg(buf[n:])
		if nn == 0 {
			err = EOF
		}
		n += nn
	}
	if n >= min {
		err = nil
	} else if n > 0 && err == EOF {
		err = ErrUnexpectedEOF
	}
	return
}

func (b *Reader) Buffered() int { return b.w - b.r }
var ErrBufferFull        = errors.New("bufio: buffer full")
func (b *Reader) ReadSliceMsg(delim byte) (line []byte, time int64, err error) {
	//fmt.Printf("%v %v %v %v %v\n", b.r, b.w, len(b.buf), b.tw, b.tr)
        s := 0 // search start index
        for {
                // Search buffer.
                if i := bytes.IndexByte(b.buf[b.r+s:b.w], delim); i >= 0 {
                        i += s
                        line = b.buf[b.r : b.r+i+1]
                        b.r += i + 1
			if b.r > b.times[b.tr].last {
				for j:= b.tr; j<b.tw; j++ {
					if b.r <= b.times[j].last {
						b.tr = j
						break
					}
				}
			}
			time = b.times[b.tr].stamp
                        break
                }

                // Pending error?
                if b.err != nil {
                        line = b.buf[b.r:b.w]
                        b.r = b.w
			b.tr = b.tw
			time = b.times[b.tr].stamp
                        err = b.readErr()
                        break
                }

                // Buffer full?
                if b.Buffered() >= len(b.buf) {
                        b.r = b.w
			b.tr = b.tw
			time = b.times[b.tr].stamp
                        line = b.buf
                        err = ErrBufferFull
                        break
                }

                s = b.w - b.r // do not rescan area we scanned before

                b.fillMsg() // buffer is not full
        }
        return
}

