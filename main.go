package main

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

type Msg struct {
	data []byte
	ch   chan []byte
}

type Connection interface {
	Write([]byte)
	Read() []byte
}

type AccountServer struct {
	conn   Connection // 数据库连接
	tokens chan chan []byte
	msg    chan Msg
}

func NewAccountServer(conn Connection, maxclientcount int) *AccountServer {
	p := &AccountServer{}
	p.conn = conn
	p.tokens = make(chan chan []byte, maxclientcount)
	p.msg = make(chan Msg, maxclientcount)
	for i := 0; i < maxclientcount; i++ {
		p.tokens <- make(chan []byte)
	}
	go p.run()
	return p
}

func (p *AccountServer) run() {
	rch := make(chan []byte)
	sch := make(chan []byte)
	go func() {
		for {
			p.conn.Write(<-sch)
		}
	}()
	go func() {
		for {
			rch <- p.conn.Read()
		}
	}()
	p.manage(sch, rch)
}

func (p *AccountServer) manage(sch chan<- []byte, rch <-chan []byte) {
	var id uint32
	players := make(map[uint32]chan []byte)
	for {
		select {
		case msg := <-p.msg:
			id++
			// 在数据包前面附上一个uint32，用于标识用户
			buff := make([]byte, 4+len(msg.data))
			buff[0] = byte(id & 0xff)
			buff[1] = byte((id >> 8) & 0xff)
			buff[2] = byte((id >> 16) & 0xff)
			buff[3] = byte((id >> 24) & 0xff)
			copy(buff[4:], msg.data)
			sch <- buff
			players[id] = msg.ch
		case data := <-rch:
			if len(data) <= 4 {
				break
			}
			// 数据库服务器返回的数据前四个字节会附带同样的uint32，用于标识用户
			var key uint32
			key = uint32(data[0])
			key |= uint32(data[1]) << 8
			key |= uint32(data[2]) << 16
			key |= uint32(data[3]) << 24
			ch, ok := players[key]
			if ok {
				ch <- data[4:]
			}
		}
	}
}

// 用户goroutine调用此函数向服务器发送数据并等待返回
func (p *AccountServer) SendAndReceive(data []byte) []byte {
	// 获取一个用于获取返回数据的信道
	ch := <-p.tokens
	// 回收信道
	defer func() { p.tokens <- ch }()
	p.msg <- Msg{data, ch}
	return <-ch
}

type MysqlConn struct {
	ch chan []byte
}

func NewMysqlConn() *MysqlConn {
	return &MysqlConn{
		ch: make(chan []byte, 10000),
	}
}

func (my *MysqlConn) Read() []byte {
	return <-my.ch
}

func (my *MysqlConn) Write(data []byte) {
	my.ch <- data
	return
}

func main() {
	mysqlConn := NewMysqlConn()
	//数据库最大并发数
	accountServer := NewAccountServer(mysqlConn, 10)

	wg := sync.WaitGroup{}
	for {
		//模拟1000个用户使用数据库连接。
		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				buff := make([]byte, 4)
				buff[0] = byte(id & 0xff)
				buff[1] = byte((id >> 8) & 0xff)
				buff[2] = byte((id >> 16) & 0xff)
				buff[3] = byte((id >> 24) & 0xff)

				recv := accountServer.SendAndReceive(buff)
				x := binary.LittleEndian.Uint32(recv)
				//id 与 x 应该相等，不等则存在问题。
				fmt.Println(id, x)
			}(i)
		}

		wg.Wait()
		time.Sleep(time.Second)
	}
}
