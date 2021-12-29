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
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	_ "github.com/lib/pq" //postgres driver
	"github.com/multiformats/go-multihash"
)

// Keccak256ToCid takes a keccak256 hash and returns its cid v1 using the provided codec.
func Keccak256ToCid(h []byte, codec uint64) (cid.Cid, error) {
	buf, err := multihash.Encode(h, multihash.KECCAK_256)
	if err != nil {
		return cid.Cid{}, err
	}
	return cid.NewCidV1(codec, multihash.Multihash(buf)), nil
}

// NewBlock takes a keccak256 hash key and the rlp []byte value it was derived from and creates an ipfs block object
func NewBlock(key, value []byte) (blocks.Block, error) {
	// we are using state codec because we don't know the codec and at this level the codec doesn't matter, the datastore key is multihash-only derived
	c, err := Keccak256ToCid(key, cid.EthStateTrie)
	if err != nil {
		return nil, err
	}
	return blocks.NewBlockWithCid(value, c)
}
