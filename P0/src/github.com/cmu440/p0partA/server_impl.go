// Implementation of a KeyValueServer. Students should write their code in this file.
// can only use ‘bufio’, ‘bytes’, ‘fmt’, ‘net’, and ‘strconv’.
package p0partA

import (

	"github.com/cmu440/p0partA/kvstore"

	"net"
	"fmt"
	"strconv"
)

type keyValueServer struct {
	activeClients int;
	kvStore kvstore.KVStore;
	deadClients int;
	server net.Listener;
}


// New creates and returns (but does not start) a new KeyValueServer.
func New(store kvstore.KVStore) KeyValueServer {
	kvServer := keyValueServer{kvStore : store}
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

	for {
		c, err := kvs.server.Accept()
		if err != nil {
			fmt.Println(err)
			return err
		}
		go handleConnection(c)
	}
	return nil
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
	kvs.server.Close()
}

func (kvs *keyValueServer) CountActive() int {
	// TODO: implement this!
	return -1
}

func (kvs *keyValueServer) CountDropped() int {
	// TODO: implement this!
	return -1
}

// TODO: add additional methods/functions below!
func handleConnection(connection  net.Conn) {

}