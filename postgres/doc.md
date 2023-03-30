## ipfs-ethdb

IPFS has been [extended](https://github.com/vulcanize/go-ipfs/releases/tag/v0.4.22-alpha) to [use Postgres](https://github.com/vulcanize/go-ipfs-config/releases/tag/v0.0.8-alpha) as a backing [datastore](https://github.com/ipfs/go-ds-sql/tree/master/postgres).
Interfacing directly with the IPFS-backing Postgres database has some advantages over using the blockservice interface.
Namely, batching of IPFS writes with other Postgres writes and avoiding lock contention on the ipfs repository (lockfile located at the `IPFS_PATH`).
The downside is that we forgo the block-exchange capabilities of the blockservice, and are only able to fetch data contained in the local datastore.


## Usage
To use this module import it and build an ethdb interface around an instance of [sqlx.DB](https://github.com/jmoiron/sqlx), you can then
employ it as you would the blockservice-based ethdbs.

```go
package main

import (
    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/core/state"
    "github.com/ethereum/go-ethereum/trie"
    "github.com/jmoiron/sqlx"
    "github.com/vulcanize/ipfs-ethdb/v5/postgres"
)

func main() {
    connectStr := "postgresql://vdbm:password@localhost:8077/cerc_testing?sslmode=disable"
    db, _ := sqlx.Connect("postgres", connectStr)

    kvs := pgipfsethdb.NewKeyValueStore(db)
    trieDB := trie.NewDatabase(kvs)
    t, _ := trie.New(common.Hash{}, trieDB)
    trieNodeIterator := t.NodeIterator([]byte{})
    // do stuff with trie node iterator

    database := pgipfsethdb.NewDatabase(db)
    stateDatabase := state.NewDatabase(database)
    stateDB, _ := state.New(common.Hash{}, stateDatabase, nil)
    stateDBNodeIterator := state.NewNodeIterator(stateDB)
    // do stuff with the statedb node iterator
}
```
