// VulcanizeDB
// Copyright © 2020 Vulcanize

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package ipfsethdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/jmoiron/sqlx"
)

// Iterator is the type that satisfies the ethdb.Iterator interface for PG-IPFS Ethereum data
// Iteratee interface is used in Geth for various tests, trie/sync_bloom.go (for fast sync),
// rawdb.InspectDatabase, and the new core/state/snapshot features.
// This should not be confused with trie.NodeIterator or state.NodeIteraor (which can be constructed
// from the ethdb.KeyValueStoreand ethdb.Database interfaces)
type Iterator struct {
	db                 *sqlx.DB
	currentKey, prefix []byte
	err                error
}

// NewIterator returns a ethdb.Iterator interface for PG-IPFS
func NewIterator(start, prefix []byte, db *sqlx.DB) ethdb.Iterator {
	return &Iterator{
		db:         db,
		prefix:     prefix,
		currentKey: start,
	}
}

// Next satisfies the ethdb.Iterator interface
// Next moves the iterator to the next key/value pair
// It returns whether the iterator is exhausted
func (i *Iterator) Next() bool {
	// this is complicated by the ipfs db keys not being the keccak256 hashes
	// go-ethereum usage of this method expects the iteration to occur over keccak256 keys
	panic("implement me: Next")
}

// Error satisfies the ethdb.Iterator interface
// Error returns any accumulated error
// Exhausting all the key/value pairs is not considered to be an error
func (i *Iterator) Error() error {
	return i.err
}

// Key satisfies the ethdb.Iterator interface
// Key returns the key of the current key/value pair, or nil if done
// The caller should not modify the contents of the returned slice
// and its contents may change on the next call to Next
func (i *Iterator) Key() []byte {
	return i.currentKey
}

// Value satisfies the ethdb.Iterator interface
// Value returns the value of the current key/value pair, or nil if done
// The caller should not modify the contents of the returned slice
// and its contents may change on the next call to Next
func (i *Iterator) Value() []byte {
	mhKey, err := MultihashKeyFromKeccak256(i.currentKey)
	if err != nil {
		i.err = err
		return nil
	}
	var data []byte
	i.err = i.db.Get(&data, getPgStr, mhKey)
	return data
}

// Release satisfies the ethdb.Iterator interface
// Release releases associated resources
// Release should always succeed and can be called multiple times without causing error
func (i *Iterator) Release() {
	i.db.Close()
}
