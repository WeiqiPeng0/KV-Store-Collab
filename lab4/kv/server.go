package kv

import (
	"context"
	"sync"
	"time"
	"log"

	"cs426.yale.edu/lab4/kv/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type toStore struct {
	data string
	ttl  time.Time
}

const loggingFlag = false

func pp(msg string, args ...interface{}) {
	if loggingFlag == true {
		log.Printf(msg, args...)
	}

}

type KvServerImpl struct {
	proto.UnimplementedKvServer
	nodeName string

	shardMap     *ShardMap
	listener     *ShardMapListener
	clientPool   ClientPool
	shutdown     chan struct{}
	data         sync.Map
	quit         chan bool
	ticker       *time.Ticker
	hostedShards []int
	mu sync.RWMutex
	keysByShard map[int]map[string]bool
}

func (server *KvServerImpl) handleShardMapUpdate() {
	// TODO: Part C
	server.mu.Lock()

	newShards := server.shardMap.ShardsForNode(server.nodeName)
	hostedShards := server.hostedShards
	

	// Find all new shards
	for _, v := range newShards {
		if containsVal(hostedShards, v) {
			continue
		} else { // update the new shard here
			err := server.updateOnShard(v)
			if err != nil {
				log.Printf("Problem when updating new shard: %v", err)
			}
		}
	}

	// Find shards to delete
	for _, v := range server.hostedShards {
		if containsVal(newShards, v) {
			continue
		} else {
			pp("Delete shard %v", v)
			server.deleteOnShard(v)
			delete(server.keysByShard, v)
			// no need to modify the list as we shall reassign later
		}
	}
	

	server.hostedShards = newShards
	server.mu.Unlock()
	pp("node %v: %v - %v", server.nodeName, server.hostedShards, server.data)


}

// helper method:
// Delete all kv pairs on shard to remove
func (server *KvServerImpl) deleteOnShard(shardIdx int) {
	pp("node %v delete upon shard %v", server.nodeName, shardIdx)
	keys := server.keysByShard[shardIdx]
	for k, _ := range keys {
		server.data.Delete(k)
		pp("Deleted the key %v \n", k)
	}
	// pp("Deleted %v \n", server.data)
}

// helper method:
// Call getShardContent() to update a new shard added.
func (server *KvServerImpl) updateOnShard(shardIdx int) (error){

	pp("node %v update upon shard %v", server.nodeName, shardIdx)

	nodes := server.shardMap.NodesForShard(shardIdx)
	// no peers contain the shard, init empty
	if len(nodes) == 0 {
		server.keysByShard[shardIdx] = make(map[string]bool)
		return status.Error(codes.NotFound, "No peers available for the shard..")
		
	}
	pp("Check AA")
	var response *proto.GetShardContentsResponse
	response =  nil
	// Otherwise, call getShardContent() until success
	for _, node := range nodes {
		if node == server.nodeName {
			continue
		}
		client, err := server.clientPool.GetClient(node)
		if err != nil {
			continue
		}
		req := &proto.GetShardContentsRequest{Shard: int32(shardIdx)}

		res, err2 := client.GetShardContents(context.Background(), req)

		if err2 == nil {
			response = res
			break
		}
	}
	pp("Check BB")

	if response == nil {
		pp("All peers fail to get shard contents...")
		server.keysByShard[shardIdx] = make(map[string]bool)
		return status.Error(codes.NotFound, "All peers fail to get shard contents...")
	}

	shardKeys := make(map[string]bool)

	for _, k := range response.Values {
		shardKeys[k.GetKey()] = true
		val := k.GetValue()
		server.data.Store(k.GetKey(), &toStore{data: val, ttl: time.Now().Add(time.Duration(k.GetTtlMsRemaining()) * time.Millisecond)})

	}

	pp("node %v shard keys: %v, data: %v", server.nodeName, shardKeys, server.data)
	server.keysByShard[shardIdx] = shardKeys


	return nil

}


// helper method:
func containsVal(s []int, v int) bool{
	for _, s := range s {
		if s == v {
			return true
		}
	}
	return false
}

func (server *KvServerImpl) shardMapListenLoop() {
	listener := server.listener.UpdateChannel()
	for {
		select {
		case <-server.shutdown:
			break
		case <-listener:
			server.handleShardMapUpdate()
		}
	}
}

func MakeKvServer(nodeName string, shardMap *ShardMap, clientPool ClientPool) *KvServerImpl {
	listener := shardMap.MakeListener()

	server := KvServerImpl{
		nodeName:   nodeName,
		shardMap:   shardMap,
		listener:   &listener,
		clientPool: clientPool,
		shutdown:   make(chan struct{}),
		quit:       make(chan bool),
		ticker:     time.NewTicker(2 * time.Second),
		keysByShard: make(map[int]map[string]bool),
	}

	server.hostedShards = make([]int, 0)
	for _, v := range server.hostedShards {
		server.keysByShard[v] = make(map[string]bool)
	}

	go func() {
		for {
			select {
			case <-server.quit:
				return
			// case <-server.listener.ch:
			// 	server.hostedShards = server.listener.shardMap.ShardsForNode(server.nodeName)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-server.quit:
				server.ticker.Stop()
				return
			case <-server.ticker.C:
				server.data.Range(func(key, value interface{}) bool {
					val := value.(*toStore)
					if time.Now().After(val.ttl) {
						server.data.Delete(key)
					}
					return true
				})
			}
		}
	}()

	go server.shardMapListenLoop()
	server.handleShardMapUpdate()
	return &server
}

func (server *KvServerImpl) Shutdown() {
	server.shutdown <- struct{}{}
	server.quit <- true
	server.listener.Close()
}

func contains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func (server *KvServerImpl) Get(
	ctx context.Context,
	request *proto.GetRequest,
) (*proto.GetResponse, error) {

	pp("node %v is getting key %v", server.nodeName, request.Key)
	// Trace-level logging for node receiving this request (enable by running with -log-level=trace),
	// feel free to use Trace() or Debug() logging in your code to help debug tests later without
	// cluttering logs by default. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Get() request")

	if request.Key == "" {
		return &proto.GetResponse{Value: "", WasFound: false}, status.Error(codes.InvalidArgument, "InvalidArgument")
	}

	server.mu.Lock()
	defer server.mu.Unlock()

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())
	// hostedShards := server.shardMap.ShardsForNode(server.nodeName)
	// pp("in Get %v, %v", server.hostedShards, shard)




	if !contains(server.hostedShards, shard) {
		pp("Not containg the shard!!!!!")
		return &proto.GetResponse{Value: "", WasFound: false}, status.Error(codes.NotFound, "NotFound")
	}

	val, ok := server.data.Load(request.Key)

	if !ok {
		return &proto.GetResponse{Value: "", WasFound: false}, nil
	} else {
		value := val.(*toStore)
		if time.Now().Before(value.ttl) {
			return &proto.GetResponse{Value: value.data, WasFound: true}, nil
		} else {
			pp("Time Expires!!!")
			server.data.Delete(request.Key)
			return &proto.GetResponse{Value: "", WasFound: false}, nil
		}
	}

	// panic("TODO: Part A")
}

func (server *KvServerImpl) Set(
	ctx context.Context,
	request *proto.SetRequest,
) (*proto.SetResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Set() request")

	// log.Println(server.nodeName)

	if request.Key == "" {
		return &proto.SetResponse{}, status.Error(codes.InvalidArgument, "InvalidArgument")
	}

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.mu.Lock()
	defer server.mu.Unlock()

	// hostedShards := server.shardMap.ShardsForNode(server.nodeName)
	if !contains(server.hostedShards, shard) {
		return &proto.SetResponse{}, status.Error(codes.NotFound, "NotFound")
	}

	server.data.Store(request.Key, &toStore{data: request.Value, ttl: time.Now().Add(time.Duration(request.TtlMs) * time.Millisecond)})

	// record key to the shard
	server.keysByShard[shard][request.Key] = true

	return &proto.SetResponse{}, nil
	// panic("TODO: Part A")
}

func (server *KvServerImpl) Delete(
	ctx context.Context,
	request *proto.DeleteRequest,
) (*proto.DeleteResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Delete() request")

	if request.Key == "" {
		return &proto.DeleteResponse{}, status.Error(codes.InvalidArgument, "InvalidArgument")
	}

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())
	// hostedShards := server.shardMap.ShardsForNode(server.nodeName)

	server.mu.Lock()
	defer server.mu.Unlock()
	if !contains(server.hostedShards, shard) {
		return &proto.DeleteResponse{}, status.Error(codes.NotFound, "NotFound")
	}

	server.data.Delete(request.Key)
	delete(server.keysByShard[shard], request.Key)

	return &proto.DeleteResponse{}, nil
	// panic("TODO: Part A")
}

func (server *KvServerImpl) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (*proto.GetShardContentsResponse, error) {

	shard := request.Shard

	// server.mu.Lock()
	// defer server.mu.Unlock()

	if !contains(server.hostedShards, int(shard)) {
		return &proto.GetShardContentsResponse{}, status.Error(codes.NotFound, "NotFound")
	}

	var allPairs []*proto.GetShardValue
	for k, _ := range server.keysByShard[int(shard)] {
		key := k
		storage, _ := server.data.Load(k)
		value := storage.(*toStore)
		val := value.data

		allPairs = append(allPairs, &proto.GetShardValue{
			Key: key,
			Value: val,
			TtlMsRemaining: value.ttl.Sub(time.Now()).Milliseconds(),
		})
	}
	res := &proto.GetShardContentsResponse{Values: allPairs}
	return res, nil





	// panic("TODO: Part C")
}
