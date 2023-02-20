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
	"github.com/ipfs/go-cid"
	_ "github.com/lib/pq" //postgres driver
	"github.com/multiformats/go-multihash"
)

// CIDFromKeccak256 converts keccak256 hash bytes into a v1 cid
func CIDFromKeccak256(hash []byte, codecType uint64) (cid.Cid, error) {
	mh, err := multihash.Encode(hash, multihash.KECCAK_256)
	if err != nil {
		return cid.Cid{}, err
	}
	return cid.NewCidV1(codecType, mh), nil
}
