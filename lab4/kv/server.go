package kv

import (
	"context"
	"sync"
	"time"

	"cs426.yale.edu/lab4/kv/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type toStore struct {
	data string
	ttl  time.Time
}

type KvServerImpl struct {
	proto.UnimplementedKvServer
	nodeName string

	shardMap   *ShardMap
	listener   *ShardMapListener
	clientPool ClientPool
	shutdown   chan struct{}
	data       sync.Map
	quit       chan bool
	ticker     *time.Ticker
}

func (server *KvServerImpl) handleShardMapUpdate() {
	// TODO: Part C
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
	}

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
	// Trace-level logging for node receiving this request (enable by running with -log-level=trace),
	// feel free to use Trace() or Debug() logging in your code to help debug tests later without
	// cluttering logs by default. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Get() request")

	if request.Key == "" {
		return &proto.GetResponse{Value: "", WasFound: false}, status.Error(codes.InvalidArgument, "InvalidArgument")
	}

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())
	hostedShards := server.shardMap.ShardsForNode(server.nodeName)
	if !contains(hostedShards, shard) {
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
	hostedShards := server.shardMap.ShardsForNode(server.nodeName)
	if !contains(hostedShards, shard) {
		return &proto.SetResponse{}, status.Error(codes.NotFound, "NotFound")
	}

	server.data.Store(request.Key, &toStore{data: request.Value, ttl: time.Now().Add(time.Duration(request.TtlMs) * time.Millisecond)})

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
	hostedShards := server.shardMap.ShardsForNode(server.nodeName)
	if !contains(hostedShards, shard) {
		return &proto.DeleteResponse{}, status.Error(codes.NotFound, "NotFound")
	}

	server.data.Delete(request.Key)

	return &proto.DeleteResponse{}, nil
	// panic("TODO: Part A")
}

func (server *KvServerImpl) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (*proto.GetShardContentsResponse, error) {
	panic("TODO: Part C")
}
