package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"runtime"
	"sync"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/spaolacci/murmur3"
)

const (
	localMongoURL          = "mongodb://localhost:9090"
	benchmarkDBName        = "benchmark"
	zHashIntegerCollection = "zhash_integer"
	zHashBinaryCollection  = "zhash_binary"
	defaultBatchSize       = 1_000
	zone                   = 0
)

type generatorFunc func(oid bson.ObjectId, zone int) Zonable

var (
	benchmarkDBCollections = map[string]generatorFunc{
		zHashIntegerCollection: func(oid bson.ObjectId, zone int) Zonable {
			return &ZHashInteger{
				ID:    oid,
				Zone:  zone,
				ZHash: hash(oid.String()),
			}
		},
		zHashBinaryCollection: func(oid bson.ObjectId, zone int) Zonable {
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], uint64(hash(oid.String())))

			return &ZHashBinary{
				ID:   oid,
				Zone: 0,
				ZHash: bson.Binary{
					Kind: 0,
					Data: b[:],
				},
			}
		},
	}
	workers        = runtime.GOMAXPROCS(0)
	insertionCount = 5_000_000
)

func main() {

	session, err := mgo.Dial(localMongoURL)
	if err != nil {
		log.Fatalf("could not connect to db: %s", err)
	}

	session.SetSafe(&mgo.Safe{})
	must(enableSharding(session))
	db := session.DB(benchmarkDBName)
	must(dropCollections(db))
	must(ensureIndexes(db, false))
	must(shardCollections(session))

	workerInsertionCount := insertionCount / workers
	overflowDocs := insertionCount % workers
	workerBatchCount := workerInsertionCount / defaultBatchSize
	overflowBatchSize := workerInsertionCount % defaultBatchSize
	if overflowBatchSize > 0 {
		workerBatchCount++
	}

	var wg sync.WaitGroup

	fmt.Printf("INFO: spawning %d workers to insert %d documents...\n", workers, insertionCount)

	for w := 0; w < workers; w++ {
		wg.Add(1)

		go func(wid int) {
			defer func() {
				wg.Done()
				fmt.Printf("INFO: worker [%d] finished inserting its documents\n", wid)
			}()
			for coll, generateModel := range benchmarkDBCollections {
				for b := 0; b < workerBatchCount; b++ {
					bulkOp := db.C(coll).Bulk()
					bulkOp.Unordered()

					batchSize := defaultBatchSize
					if b == workerBatchCount-1 {
						if overflowBatchSize > 0 {
							batchSize = overflowBatchSize
						}

						// one of the workers has to take care of the overflow documents that could not be evenly distributed
						// amongst the worker
						if overflowDocs > 0 && wid == 0 {
							batchSize += overflowDocs
						}
					}

					for i := 0; i < batchSize; i++ {
						bulkOp.Insert(generateModel(bson.NewObjectId(), zone))
					}

					if _, err := bulkOp.Run(); err != nil {
						log.Fatalf("FATAL: failed to perform bulk insertion into the '%s' collection: %s", coll, err)
					}
				}
			}
		}(w)
	}

	fmt.Println("INFO: waiting for workers...")
	wg.Wait()
	fmt.Println("INFO: workers finished, computing results...")
	must(ensureIndexes(db, true))

	collections, err := db.CollectionNames()
	if err != nil {
		log.Fatalf("FATAL: failed to fetch collections: %s", err)
	}

	for _, coll := range collections {
		var result bson.M
		if err := db.Run(bson.D{{Name: "collStats", Value: coll}}, &result); err != nil {
			log.Fatalf("FATAL: failed to run 'collStats' command: %s", err)
		}

		fmt.Println("-----------------------------------------------")
		fmt.Printf("Index sizes for collection '%s' - total index size: %d bytes:\n", coll, result["totalIndexSize"])
		indexes, ok := result["indexSizes"].(bson.M)
		if !ok {
			log.Fatalf("invalid type: %T", result["indexSizes"])
		}
		for name, size := range indexes {
			fmt.Printf("\t%s=%d bytes\n", name, size)
		}
		fmt.Println("-----------------------------------------------")
	}
}

func dropCollections(db *mgo.Database) error {
	collections, err := db.CollectionNames()
	if err != nil {
		return fmt.Errorf("failed to fetch collection names: %w", err)
	}

	for _, c := range collections {
		if err := db.C(c).DropCollection(); err != nil {
			return fmt.Errorf("failed to drop the '%s' collection: %w", c, err)
		}
	}

	return nil
}

func enableSharding(session *mgo.Session) error {
	if err := session.Run(
		bson.D{
			{Name: "enableSharding", Value: benchmarkDBName},
		},
		nil,
	); err != nil {
		return fmt.Errorf("failed to enable sharding: %s", err)
	}

	return nil
}

func shardCollections(session *mgo.Session) error {
	for coll := range benchmarkDBCollections {
		if err := session.Run(
			bson.D{
				{Name: "shardCollection", Value: fmt.Sprintf("%s.%s", benchmarkDBName, coll)},
				{Name: "key", Value: bson.D{
					{Name: "zone", Value: 1},
					{Name: "zhash", Value: 1}},
				},
				{Name: "unique", Value: false},
			},
			nil,
		); err != nil {
			return fmt.Errorf("failed to shard collection '%s': %s", coll, err)
		}
	}

	return nil
}

func ensureIndexes(db *mgo.Database, background bool) error {
	for coll := range benchmarkDBCollections {
		if err := db.C(coll).EnsureIndex(mgo.Index{
			Key: []string{
				"zone",
				"zhash",
			},
			Unique:     true,
			Background: background,
		}); err != nil {
			return fmt.Errorf("failed to create index on collection '%s': %s", coll, err)
		}
	}

	return nil
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func hash(v string) int {
	return int(murmur3.Sum64([]byte(v)) & 0x7FFFFFFFFFFFFFFF)
}
