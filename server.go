package main

import (
	"bufio"
	"fmt"
	"github.com/agrawalpooja/assignment4/fs"
	"github.com/agrawalpooja/assignment4/raft"
	"net"
	"os"
	"strconv"
	"encoding/json"
)

var crlf = []byte{'\r', '\n'}

func check(obj interface{}) {
	if obj != nil {
		fmt.Println(obj)
		os.Exit(1)
	}
}

func reply(conn *net.TCPConn, msg *fs.Msg, LId int) bool {
	var err error
	write := func(data []byte) {
		if err != nil {
			return
		}
		_, err = conn.Write(data)
	}
	var resp string
	switch msg.Kind {
	case 'C': // read response
		resp = fmt.Sprintf("CONTENTS %d %d %d", msg.Version, msg.Numbytes, msg.Exptime)
	case 'O':
		resp = "OK "
		if msg.Version > 0 {
			resp += strconv.Itoa(msg.Version)
		}
	case 'F':
		resp = "ERR_FILE_NOT_FOUND"
	case 'V':
		resp = "ERR_VERSION " + strconv.Itoa(msg.Version)
	case 'M':
		resp = "ERR_CMD_ERR"
	case 'I':
		resp = "ERR_INTERNAL"
	case 'R':
		resp = "ERR_REDIRECT " + strconv.Itoa(LId)
	default:
		fmt.Printf("Unknown response kind '%c'", msg.Kind)
		return false
	}
	resp += "\r\n"
	write([]byte(resp))
	if msg.Kind == 'C' {
		write(msg.Contents)
		write(crlf)
	}
	return err == nil
}

func serve(rn *raft.RaftNode, conn *net.TCPConn) {
	reader := bufio.NewReader(conn)
	for {
		msg, msgerr, fatalerr := fs.GetMsg(reader)
		if fatalerr != nil || msgerr != nil {
			reply(conn, &fs.Msg{Kind: 'M'}, -1)
			conn.Close()
			break
		}

		if msgerr != nil {
			if (!reply(conn, &fs.Msg{Kind: 'M'}, -1)) {
				conn.Close()
				break
			}
		}
		//append to raft
		if string(msg.Kind)!= "r" {
			msgData,err := json.Marshal(msg)
			check(err)
			if rn.Sm.State == "leader"{
				rn.Append(msgData)
				ci := <-rn.CommitChannel();
				com :=ci.(raft.CommitEv)
			
				if com.Err != nil {
					reply(conn, &fs.Msg{Kind: 'I'}, -1)
					conn.Close()
					break
				}
				err = json.Unmarshal(com.Data.Data,&msg)
				check(err)
			}else{
				//redirect
				reply(conn, &fs.Msg{Kind: 'R'}, rn.Sm.LeaderId)
				conn.Close()
				break
			}

		}
		response := fs.ProcessMsg(msg)
		if !reply(conn, response, -1) {
			conn.Close()
			break
		}
	}
}

func serverMain(rn *raft.RaftNode, addr string) {
	tcpaddr, err := net.ResolveTCPAddr("tcp", addr)
	check(err)
	tcp_acceptor, err := net.ListenTCP("tcp", tcpaddr)
	check(err)
	go func(rfnode *raft.RaftNode) {
		for {	//listen to client
			tcp_conn, err := tcp_acceptor.AcceptTCP()
			check(err)
			go serve(rfnode, tcp_conn)
		}
	}(rn)
}

func main() {
}
