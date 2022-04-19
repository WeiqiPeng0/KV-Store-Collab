package kv

import (
	"context"
	"errors"
	"sync"
	"time"

	kvpb "cs426.yale.edu/lab4/kv/proto"
	"github.com/sirupsen/logrus"
)

type Kv struct {
	shardMap   *ShardMap
	clientPool ClientPool

	// Add any client-side state you want here
	numRequest int
}

func MakeKv(shardMap *ShardMap, clientPool ClientPool) *Kv {
	kv := &Kv{
		shardMap:   shardMap,
		clientPool: clientPool,
	}
	// Add any initialization logic
	kv.numRequest = 0
	return kv
}

func (kv *Kv) Get(ctx context.Context, key string) (string, bool, error) {
	// Trace-level logging -- you can remove or use to help debug in your tests
	// with `-log-level=trace`. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Get() request")

	// panic("TODO: Part B")

	kv.numRequest++

	// Get the hosting nodes for the shard corresponding to key.
	shard := GetShardForKey(key, kv.shardMap.NumShards())
	nodes := kv.shardMap.NodesForShard(shard)

	// Error handling for shard with no hosting nodes.
	if len(nodes) == 0 {
		return "", false, errors.New("no nodes are available")
	}

	// GRPC call GetRequest
	req := kvpb.GetRequest{Key: key}
	var getClientErr error
	var clientGetErr error
	var errorDefault error

	// Failovers and error handling
	for i := 0; i < len(nodes); i++ {
		node_idx := (kv.numRequest - 1 + i) % len(nodes)
		client, err := kv.clientPool.GetClient(nodes[node_idx])
		if err != nil {
			getClientErr = err
			continue
		}
		res, err := client.Get(ctx, &req)
		if err != nil {
			clientGetErr = err
			continue
		}
		return res.Value, res.WasFound, nil
	}

	// All nodes failed
	if getClientErr != nil {
		logrus.Debug("getClientErr =", getClientErr)
		return "", false, getClientErr
	}
	if clientGetErr != nil {
		logrus.Debug("clientGetErr =", clientGetErr)
		return "", false, clientGetErr
	}

	// Should not reach here.
	logrus.Debug("errorDefault =", errorDefault)
	return "", false, errorDefault

	// client, getClientErr := kv.clientPool.GetClient(nodes[(kv.numRequest-1)%len(nodes)])
	// if getClientErr != nil {
	// 	return "", false, getClientErr
	// }
	// res, clientGetErr := client.Get(ctx, &req)
	// if clientGetErr != nil {
	// 	return "", false, clientGetErr
	// }
	// return res.Value, res.WasFound, nil
}

func (kv *Kv) SetNode(ctx context.Context, node string, req *kvpb.SetRequest) error {
	client, err := kv.clientPool.GetClient(node)
	if err != nil {
		return err
	}
	_, err = client.Set(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (kv *Kv) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Set() request")

	// panic("TODO: Part B")

	// Get the hosting nodes for the shard corresponding to key.
	shard := GetShardForKey(key, kv.shardMap.NumShards())
	nodes := kv.shardMap.NodesForShard(shard)

	// Error handling for shard with no hosting nodes.
	if len(nodes) == 0 {
		return errors.New("no nodes are available")
	}

	var setError error
	var wg sync.WaitGroup
	// GRPC call SetRequest
	req := kvpb.SetRequest{Key: key, Value: value, TtlMs: ttl.Milliseconds()}
	for _, node := range nodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			err := kv.SetNode(ctx, node, &req)
			if err != nil {
				setError = err
			}
		}(node)
	}
	wg.Wait()
	return setError
}

func (kv *Kv) DeleteFromNode(ctx context.Context, node string, req *kvpb.DeleteRequest) error {
	client, err := kv.clientPool.GetClient(node)
	if err != nil {
		return err
	}
	_, err = client.Delete(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (kv *Kv) Delete(ctx context.Context, key string) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Delete() request")

	// panic("TODO: Part B")
	// Get the hosting nodes for the shard corresponding to key.
	shard := GetShardForKey(key, kv.shardMap.NumShards())
	nodes := kv.shardMap.NodesForShard(shard)

	// Error handling for shard with no hosting nodes.
	if len(nodes) == 0 {
		return errors.New("no nodes are available")
	}

	var deleteError error
	var wg sync.WaitGroup
	// GRPC call DeleteRequest
	req := kvpb.DeleteRequest{Key: key}
	for _, node := range nodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			err := kv.DeleteFromNode(ctx, node, &req)
			if err != nil {
				deleteError = err
			}
		}(node)
	}
	wg.Wait()
	return deleteError
}
