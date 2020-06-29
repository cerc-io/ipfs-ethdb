// VulcanizeDB
// Copyright © 2019 Vulcanize

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
	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-ipfs-ds-help"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" //postgres driver
	"github.com/multiformats/go-multihash"
)

// MultihashKeyFromKeccak256 converts keccak256 hash bytes into a blockstore-prefixed multihash db key string
func MultihashKeyFromKeccak256(h []byte) (string, error) {
	mh, err := multihash.Encode(h, multihash.KECCAK_256)
	if err != nil {
		return "", err
	}
	dbKey := dshelp.MultihashToDsKey(mh)
	return blockstore.BlockPrefix.String() + dbKey.String(), nil
}

// TestDB connect to the testing database
// it assumes the database has the IPFS public.blocks table present
// DO NOT use a production db for the test db, as it will remove all contents of the public.blocks table
func TestDB() (*sqlx.DB, error) {
	connectStr := "postgresql://localhost:5432/vulcanize_testing?sslmode=disable"
	return sqlx.Connect("postgres", connectStr)
}

// ResetTestDB drops all rows in the test db public.blocks table
func ResetTestDB(db *sqlx.DB) error {
	_, err := db.Exec("TRUNCATE public.blocks")
	return err
}

// Keccak256ToCid takes a keccak256 hash and returns its cid v0 using the provided codec.
func Keccak256ToCid(h []byte) (cid.Cid, error) {
	buf, err := multihash.Encode(h, multihash.KECCAK_256)
	if err != nil {
		return cid.Cid{}, err
	}
	return cid.NewCidV0(multihash.Multihash(buf)), nil
}

// NewBlock takes a keccak256 hash key and the rlp []byte value it was derived from and creates an ipfs block object
func NewBlock(key, value []byte) (blocks.Block, error) {
	c, err := Keccak256ToCid(key) // we are using cidv0 because we don't know the codec and codec doesn't matter, the datastore key is multihash-only derived
	if err != nil {
		return nil, err
	}
	return blocks.NewBlockWithCid(value, c)
}
