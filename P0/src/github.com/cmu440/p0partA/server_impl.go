// Implementation of a KeyValueServer. Students should write their code in this file.
// can only use ‘bufio’, ‘bytes’, ‘fmt’, ‘net’, and ‘strconv’.
package p0partA

import (
	"github.com/cmu440/p0partA/kvstore"

	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

const (
	BUFFER_SIZE = 500
)

type KVOP int

const (
	PUT = iota
	GET
	DELETE
)

type clientInfo struct {
	id              int
	connection      net.Conn
	writeChannel    chan string
	readChannel     chan string
	finishedReading chan bool
	finishedWriting chan bool
	retKV           chan keyValue
}

type keyValueServer struct {
	clientMap          map[int]clientInfo
	kvStore            kvstore.KVStore
	server             net.Listener
	connectionChannel  chan net.Conn
	activeCountChannel chan int
	deadCountChannel   chan int
	closeClientChannel chan int
	kvsChannel         chan kvOp
	closeEverything    chan bool
	nextId             int
	deadClients        int
}

type kvOp struct {
	op       KVOP
	clientId int
	kv       keyValue
}

type keyValue struct {
	key string
	val []([]byte)
}

// New creates and returns (but does not start) a new KeyValueServer.
func New(store kvstore.KVStore) KeyValueServer {
	kvServer := keyValueServer{kvStore: store,
		connectionChannel:  make(chan net.Conn),
		activeCountChannel: make(chan int),
		deadCountChannel:   make(chan int),
		clientMap:          make(map[int]clientInfo),
		kvsChannel:         make(chan kvOp),
		closeClientChannel: make(chan int),
	}
	return &kvServer
}

func (kvs *keyValueServer) Start(port int) error {
	// TODO: implement this!

	// start a server in the given port
	connString := ":" + strconv.Itoa(port)

	var err error
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
	return <-kvs.activeCountChannel
}

func (kvs *keyValueServer) CountDropped() int {
	kvs.deadCountChannel <- 0
	return <-kvs.deadCountChannel
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
			kvs.nextId++

			fmt.Println("New connection received")
			client := clientInfo{
				connection:      conn,
				id:              kvs.nextId,
				readChannel:     make(chan string),
				writeChannel:    make(chan string, BUFFER_SIZE),
				finishedReading: make(chan bool),
				finishedWriting: make(chan bool),
				retKV:           make(chan keyValue),
			}
			kvs.clientMap[client.id] = client
			go client.writeToClient(kvs.kvsChannel, kvs.closeClientChannel)
			go client.readFromClient(kvs.kvsChannel, kvs.closeClientChannel)

		case <-kvs.activeCountChannel:
			count := 0
			for range kvs.clientMap {
				count += 1
			}
			kvs.activeCountChannel <- count

		case <-kvs.deadCountChannel:
			kvs.deadCountChannel <- kvs.deadClients

		case op := <-kvs.kvsChannel:
			retChannelForClient := kvs.clientMap[op.clientId].retKV

			var retkv keyValue
			switch op.op {
			case GET:

				val := kvs.kvStore.Get(op.kv.key)
				fmt.Printf("GOT VALUE-> for key %s, %v",op.kv.key, val)
				retkv = keyValue{key: op.kv.key, val: val}
				break
			case PUT:
				retkv = keyValue{}
				kvs.kvStore.Put(op.kv.key, op.kv.val[0])
				break
			case DELETE:
				retkv = keyValue{}
				kvs.kvStore.Clear(op.kv.key)
				break
			}

			retChannelForClient <- retkv

		case clientId := <-kvs.closeClientChannel:
			kvs.clientMap[clientId].connection.Close()
			delete(kvs.clientMap, clientId)
			kvs.deadClients++
		}
	}
}

func (client *clientInfo) writeToClient(kvsChannel chan kvOp, closeClientChannel chan int) {
	writer := bufio.NewWriter(client.connection)
	for {
		select {
		case <-client.finishedReading:
			closeClientChannel <- client.id
			return
		case retKv := <-client.retKV:
			// write to client
			fmt.Printf(retKv.key)

			if len(retKv.val) >  0 {
				fmt.Printf(string(retKv.val[0]))
			}

			fmt.Printf("%v", retKv.val)

			if retKv.val != nil {
				for _, val := range retKv.val {
					writeStr := retKv.key + ":" + string(val) + "\n"
					fmt.Printf("Returining:-> " + writeStr)
					writer.WriteString(writeStr)
				}
			}

		}
	}

}

func (client *clientInfo) readFromClient(kvsChannel chan kvOp, closeClientChannel chan int) {

	reader := bufio.NewReader(client.connection)
	for {
		// Read up to and including the first '\n' character.
		msgBytes, err := reader.ReadBytes('\n')

		if err == io.EOF {
			fmt.Printf("client %d read EOF", client.id)
			client.finishedReading <- true
			return
		}
		if err != nil {
			fmt.Errorf("There was an error while reading from client")
			return
		}

		readMsg := string(msgBytes)
		fmt.Printf("Message %s ", readMsg)
		kvsChannel <- client.parse(readMsg)
	}
}

func (client *clientInfo) parse(msg string) kvOp {
	s := strings.Split(msg, ":")
	operation := s[0]
	var storeOp kvOp
	kv := keyValue{key: s[1]}
	kv.val = make([]([]byte), 0)

	switch operation {
	case "Put":
		kv.val = append(kv.val, []byte(s[2]))
		storeOp = kvOp{clientId: client.id, op: PUT, kv: kv}
		break
	case "Get":
		storeOp = kvOp{clientId: client.id, op: GET, kv: kv}
		break
	case "Delete":
		storeOp = kvOp{clientId: client.id, op: DELETE, kv: kv}
		break
	}
	fmt.Printf("storep is -> %+v\n", storeOp)
	return storeOp
}
