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

type Batch struct {
	db   *sqlx.DB
	tx   *sqlx.Tx
	size int
}

func NewBatch(db *sqlx.DB) ethdb.Batch {
	return &Batch{
		db: db,
	}
}

// Put satisfies the ethdb.Batch interface
// Put inserts the given value into the key-value data store
func (b *Batch) Put(key []byte, value []byte) (err error) {
	if b.tx == nil {
		b.Reset()
	}
	mhKey, err := MultihashKeyFromKeccak256(key)
	if err != nil {
		return err
	}
	if _, err = b.tx.Exec(putPgStr, mhKey, value); err != nil {
		return err
	}
	b.size += len(value)
	return nil
}

// Delete satisfies the ethdb.Batch interface
// Delete removes the key from the key-value data store
func (b *Batch) Delete(key []byte) (err error) {
	if b.tx == nil {
		b.Reset()
	}
	mhKey, err := MultihashKeyFromKeccak256(key)
	if err != nil {
		return err
	}
	_, err = b.tx.Exec(deletePgStr, mhKey)
	return err
}

// ValueSize satisfies the ethdb.Batch interface
// ValueSize retrieves the amount of data queued up for writing
// The returned value is the total byte length of all data queued to write
func (b *Batch) ValueSize() int {
	return b.size
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
	b.size = 0
}
