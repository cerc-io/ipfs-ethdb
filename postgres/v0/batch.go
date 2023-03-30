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

package pgipfsethdb

import (
	"math/big"

	"github.com/ipfs/go-cid"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/jmoiron/sqlx"
)

var _ ethdb.Batch = &Batch{}

// Batch is the type that satisfies the ethdb.Batch interface for PG-IPFS Ethereum data using a direct Postgres connection
type Batch struct {
	db        *sqlx.DB
	tx        *sqlx.Tx
	valueSize int

	blockNumber *big.Int
}

// NewBatch returns a ethdb.Batch interface for PG-IPFS
func NewBatch(db *sqlx.DB, tx *sqlx.Tx, blockNumber *big.Int) ethdb.Batch {
	b := &Batch{
		db:          db,
		tx:          tx,
		blockNumber: blockNumber,
	}
	if tx == nil {
		b.Reset()
	}
	return b
}

// Put satisfies the ethdb.Batch interface
// Put inserts the given value into the key-value data store
// Key is expected to be a fully formulated cid key
// TODO: note, now that we expected a cid we could route to the "cids" tables based on prefix instead of to public.blocks
// but is it better to handle this routing here, or use a completely different interface since we already have to refactor
// at levels above this package in order to pass in cids instead of raw keccak256 hashes
func (b *Batch) Put(cidBytes []byte, value []byte) (err error) {
	// cast and resolve strings from cid.Cast
	// this will assert that we have a correctly formatted CID
	// and will handle the different string encodings for v0 and v1 CIDs
	// (note that this v0 vs v1 is different from the blockstore v0 vs v1)
	c, err := cid.Cast(cidBytes)
	if err != nil {
		return err
	}
	if _, err = b.tx.Exec(putPgStr, c.String(), value, b.blockNumber.Uint64()); err != nil {
		return err
	}
	b.valueSize += len(value)
	return nil
}

// Delete satisfies the ethdb.Batch interface
// Delete removes the key from the key-value data store
func (b *Batch) Delete(cidBytes []byte) (err error) {
	c, err := cid.Cast(cidBytes)
	if err != nil {
		return err
	}
	_, err = b.tx.Exec(deletePgStr, c.String())
	return err
}

// ValueSize satisfies the ethdb.Batch interface
// ValueSize retrieves the amount of data queued up for writing
// The returned value is the total byte length of all data queued to write
func (b *Batch) ValueSize() int {
	return b.valueSize
}

// Write satisfies the ethdb.Batch interface
// Write flushes any accumulated data to disk
func (b *Batch) Write() error {
	if b.tx == nil {
		return nil
	}
	return b.tx.Commit()
}

// Replay satisfies the ethdb.Batch interface
// Replay replays the batch contents
func (b *Batch) Replay(w ethdb.KeyValueWriter) error {
	return errNotSupported
}

// Reset satisfies the ethdb.Batch interface
// Reset resets the batch for reuse
// This should be called after every write
func (b *Batch) Reset() {
	var err error
	b.tx, err = b.db.Beginx()
	if err != nil {
		panic(err)
	}
	b.valueSize = 0
}
