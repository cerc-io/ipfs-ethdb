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

package pgipfsethdb

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-ipfs-ds-help"
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

// DatastoreKeyFromGethKey returns the public.blocks key from the provided geth key
func DatastoreKeyFromGethKey(h []byte) (string, error) {
	keyType, keyComponents := ResolveKeyType(h)
	switch keyType {
	case Keccak:
		return MultihashKeyFromKeccak256(h)
	case Header:
		return MultihashKeyFromKeccak256(keyComponents[1])
	case Preimage:
		return MultihashKeyFromKeccak256(keyComponents[1])
	case Prefixed, Suffixed:
		// This data is not mapped by hash => content by geth, store it using the prefixed/suffixed key directly
		// I.e. the public.blocks datastore key == the hex representation of the geth key
		return common.Bytes2Hex(h), nil
	default:
		return "", fmt.Errorf("invalid formatting of database key: %x", h)
	}

}
