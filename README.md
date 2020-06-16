## pg-ipfs-ethdb


> go-ethereum ethdb interfaces for Ethereum state data stored in Postgres-backed IPFS

## Background

Go-ethereum defines a number of interfaces in the [ethdb package](https://github.com/ethereum/go-ethereum/tree/master/ethdb) for
interfacing with a state database. These interfaces are used to build higher-level types such as the [trie.Database](https://github.com/ethereum/go-ethereum/blob/master/trie/database.go#L77)
which are used to perform the bulk of state related needs.

Ethereum data can be stored on IPFS, standard codecs for Etheruem data are defined in the [go-cid](https://github.com/ipfs/go-cid) library. Here at Vulcanize we
have [extended IPFS](https://github.com/vulcanize/go-ipfs/releases/tag/v0.4.22-alpha) to [use Postgres](https://github.com/vulcanize/go-ipfs-config/releases/tag/v0.0.8-alpha) as a backing database.
Additionally, [we have extended go-ethereum](https://github.com/vulcanize/go-ethereum/releases/tag/v1.9.11-statediff-0.0.2) to enable the efficient export of state data in the form of state diff objects.
Together, this allows us to store all Ethereum data on Postgres-backed IPFS.

Geth stores state data in leveldb as key-value pairs between the keccak256 hash of the rlp-encoded object and the rlp-encoded object.
Ethereum data on IPFS is also stored as key-value pairs with the value being the rlp-encoded byte value for the object,
but the key is a derivative of the keccak256 hash rather than the hash itself. This library provides
ethdb interfaces for Ethereum data on IPFS by handling the conversion of a keccak256 hash to its multihash-derived key.


## Usage
To use this module simply import it and build the desired interface around an instance of [sqlx.DB](https://github.com/jmoiron/sqlx), you can then
employ it as you would the usual [leveldb](https://github.com/ethereum/go-ethereum/tree/master/ethdb/leveldb) or [memorydb](https://github.com/ethereum/go-ethereum/tree/master/ethdb/memorydb) interfaces
with a few exceptions:

```go
package main

import (
	"github.com/ethereum/go-ethereum/trie"
	"github.com/jmoiron/sqlx"
	"github.com/vulcanize/pg-ipfs-ethdb"
)

func main() {
    connectStr := "postgresql://localhost:5432/vulcanize_testing?sslmode=disable"
    db, _ := sqlx.Connect("postgres", connectStr)

    kvs := ipfsethdb.NewKeyValueStore(db)
    trieDB := trie.NewDatabase(kvs)

    // do stuff
}
```

EXCEPTIONS: AncientReader, AncientWriter, and Iteratee interfaces are not functionally complete.

Ancient data does not currently have a representation on IPFS, and recapitulation of the database key iterator is complicated since go-ethereum
types that use this interface expect the iterator to iterate over keccak256 hash keys, whereas the keys for Ethereum data on IPFS are derived from that hash but not the hash itself.

## Maintainers
@vulcanize
@AFDudley
@i-norden

## Contributing
Contributions are welcome!

VulcanizeDB follows the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/1/4/code-of-conduct).

## License
[AGPL-3.0](LICENSE) © Vulcanize Inc
