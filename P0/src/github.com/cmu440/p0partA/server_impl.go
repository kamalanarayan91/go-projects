// Implementation of a KeyValueServer. Students should write their code in this file.
// can only use ‘bufio’, ‘bytes’, ‘fmt’, ‘net’, and ‘strconv’.
package p0partA

import (

	"github.com/cmu440/p0partA/kvstore"

	"net"
	"fmt"
	"strconv"
)

const (
	BUFFER_SIZE  = 500
)

type clientInfo struct {
	id int;
	connection net.Conn
	writeChannel chan string
	readChannel chan string
}

type keyValueServer struct {
	clientMap map[int]clientInfo
	kvStore kvstore.KVStore;
	server net.Listener;
	connectionChannel chan net.Conn
	activeCountChannel chan int
	deadCountChannel chan int
	kvsChannel chan string
	nextId int
	deadClients int
}


// New creates and returns (but does not start) a new KeyValueServer.
func New(store kvstore.KVStore) KeyValueServer {
	kvServer := keyValueServer{kvStore : store,
		connectionChannel: make(chan net.Conn),
		activeCountChannel: make(chan int),
		clientMap: make(map[int]clientInfo),

	}
	return &kvServer
}

func (kvs *keyValueServer) Start(port int) error {
	// TODO: implement this!

	// start a server in the given port
	connString := ":" + strconv.Itoa(port);

	var err error;
	kvs.server, err = net.Listen("tcp", connString)

	if err != nil {
		fmt.Println(err)
		return err
	}

	go kvs.acceptClients()
	go kvs.process()
	return nil
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
	kvs.server.Close()
}

func (kvs *keyValueServer) CountActive() int {
	kvs.activeCountChannel <- 0
	return <- kvs.activeCountChannel
}

func (kvs *keyValueServer) CountDropped() int {
	kvs.deadCountChannel <- 0
	return <- kvs.deadCountChannel
}

// TODO: add additional methods/functions below!
func (kvs *keyValueServer) acceptClients() {
	for {
		c, err := kvs.server.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		kvs.connectionChannel <- c
	}
}

func (kvs *keyValueServer) process() {

	for {
		select {

		case conn := <-kvs.connectionChannel:
			kvs.nextId++;

			fmt.Println("New connection received")
			client := clientInfo{
				connection : conn,
				id: kvs.nextId,
				readChannel: make(chan string),
				writeChannel: make(chan string, BUFFER_SIZE),
			}
			kvs.clientMap[client.id]  = client
			go client.writeToClient()
			go client.readFromClient()

		case  <- kvs.activeCountChannel:
			count := 0;
			for range kvs.clientMap {
				count += 1;
			}
			kvs.activeCountChannel <- count
		case count := <- kvs.deadCountChannel:
			kvs.deadClients += count
			kvs.deadCountChannel <- kvs.deadClients
		}
	}
}

func (client *clientInfo) writeToClient() {

}

func (client *clientInfo) readFromClient() {

}

