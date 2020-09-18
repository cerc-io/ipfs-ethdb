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

import "bytes"

type KeyType uint

const (
	Invalid KeyType = iota
	Static
	Keccak
	Prefixed
	Suffixed
	Header
	Preimage
)

var (
	// KeyDelineation is used to delineate the key prefixes and suffixes
	KeyDelineation = []byte("-fix-")

	// NumberDelineation is used to delineate the block number encoded in a key
	NumberDelineation = []byte("-nmb-")

	// Data item prefixes (use single byte to avoid mixing data types, avoid `i`, used for indexes).
	HeaderPrefix   = []byte("h")           // headerPrefix + num (uint64 big endian) + hash -> header
	PreimagePrefix = []byte("secure-key-") // preimagePrefix + hash -> preimage
)

// ResolveKeyType returns the key type based on the prefix
func ResolveKeyType(key []byte) (KeyType, [][]byte) {
	sk := bytes.Split(key, KeyDelineation)

	// these heuristics are reliant on the current db key patterns
	switch len(sk) {
	case 1:
		if len(sk[0]) < 32 {
			return Static, sk
		}
		return Keccak, sk
	case 2:
		switch prefix := sk[0]; {
		case bytes.Equal(prefix, HeaderPrefix):
			return Header, bytes.Split(sk[1], NumberDelineation)
		case bytes.Equal(prefix, PreimagePrefix):
			return Preimage, sk
		default:
			return Prefixed, sk
		}
	case 3:
		return Suffixed, sk
	}
	return Invalid, sk
}
