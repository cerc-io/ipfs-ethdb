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

package pgipfsethdb_test

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/jmoiron/sqlx"
	"github.com/mailgun/groupcache/v2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
	sdtypes "github.com/ethereum/go-ethereum/statediff/types"
	"github.com/multiformats/go-multihash"

	pgipfsethdb "github.com/cerc-io/ipfs-ethdb/v4/postgres"
)

var (
	database           ethdb.Database
	db                 *sqlx.DB
	err                error
	testBlockNumber    = big.NewInt(1337)
	testHeader         = types.Header{Number: testBlockNumber}
	testHeaderID       = "1337beef"
	testHeaderKey      = testHeader.Hash().Bytes()
	testHeaderValue, _ = rlp.EncodeToBytes(&testHeader)
	testHeaderMhKey, _ = pgipfsethdb.MultihashKeyFromKeccak256(testHeaderKey)
	testHeaderCID, _   = ipld.RawdataToCid(ipld.MEthStateTrie, testHeaderValue, multihash.KECCAK_256)

	testAccountPK, _    = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	testAccountAddr     = crypto.PubkeyToAddress(testAccountPK.PublicKey) //0x703c4b2bD70c169f5717101CaeE543299Fc946C7
	testAccountLeafKey  = crypto.Keccak256Hash(testAccountAddr.Bytes())
	testAccountPath     = []byte{testAccountLeafKey[0] & 0xf0}
	testAccountCodeHash = common.HexToHash("0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")

	testAccount, _ = rlp.EncodeToBytes(&types.StateAccount{
		Nonce:    uint64(0),
		Balance:  big.NewInt(1000),
		CodeHash: testAccountCodeHash.Bytes(),
		Root:     common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"),
	})
	testAccountLeafNode, _ = rlp.EncodeToBytes(&[]interface{}{
		testAccountLeafKey,
		testAccount,
	})
	testAccountCID, _ = ipld.RawdataToCid(ipld.MEthStateTrie, testAccountLeafNode, multihash.KECCAK_256)
	testAccountMhKey  = shared.MultihashKeyFromCID(testAccountCID)
	testAccountKey    = crypto.Keccak256Hash(testAccountLeafNode).Bytes()

	storageLocation        = common.HexToHash("0")
	testStorageLeafKey     = crypto.Keccak256Hash(storageLocation[:])
	testStorageValue       = crypto.Keccak256([]byte{1, 2, 3, 4, 5})
	testStoragePartialPath = common.Hex2Bytes("20290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563")
	testStorageStatePath   = []byte{} // TODO

	testStorageLeafNode, _ = rlp.EncodeToBytes(&[]interface{}{
		testStoragePartialPath,
		testStorageValue,
	})
	testStorageCID, _ = ipld.RawdataToCid(ipld.MEthStorageTrie, testStorageLeafNode, multihash.KECCAK_256)
	testStorageMhKey  = shared.MultihashKeyFromCID(testStorageCID)
	testStorageKey    = crypto.Keccak256Hash(testStorageLeafNode).Bytes()
)

var _ = Describe("Database", func() {
	BeforeEach(func() {
		db, err = pgipfsethdb.TestDB()
		Expect(err).ToNot(HaveOccurred())

		cacheConfig := pgipfsethdb.CacheConfig{
			Name:           "db",
			Size:           3000000, // 3MB
			ExpiryDuration: time.Hour,
		}

		database = pgipfsethdb.NewDatabase(db, cacheConfig, false)

		databaseWithBlock, ok := database.(*pgipfsethdb.Database)
		Expect(ok).To(BeTrue())
		(*databaseWithBlock).BlockNumber = testBlockNumber
	})
	AfterEach(func() {
		groupcache.DeregisterGroup("db")
		err = pgipfsethdb.ResetTestDB(db)
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("Has", func() {
		It("returns false if a key-pair doesn't exist in the db", func() {
			has, err := database.Has(testHeaderKey)
			Expect(err).ToNot(HaveOccurred())
			Expect(has).ToNot(BeTrue())
		})
		It("returns true if a key-pair exists in the db", func() {
			_, err = db.Exec("INSERT into public.blocks (key, data, block_number) VALUES ($1, $2, $3)", testHeaderMhKey, testHeaderValue, testBlockNumber.Uint64())
			Expect(err).ToNot(HaveOccurred())
			has, err := database.Has(testHeaderKey)
			Expect(err).ToNot(HaveOccurred())
			Expect(has).To(BeTrue())
		})
	})

	Describe("Get", func() {
		It("throws an err if the key-pair doesn't exist in the db", func() {
			_, err = database.Get(testHeaderKey)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("sql: no rows in result set"))
		})
		It("returns the value associated with the key, if the pair exists", func() {
			_, err = db.Exec("INSERT into public.blocks (key, data, block_number) VALUES ($1, $2, $3)", testHeaderMhKey, testHeaderValue, testBlockNumber.Uint64())
			Expect(err).ToNot(HaveOccurred())
			val, err := database.Get(testHeaderKey)
			Expect(err).ToNot(HaveOccurred())
			Expect(val).To(Equal(testHeaderValue))
		})
	})

	Describe("Put", func() {
		It("persists the key-value pair in the database", func() {
			_, err = database.Get(testHeaderKey)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("sql: no rows in result set"))

			err = database.Put(testHeaderKey, testHeaderValue)
			Expect(err).ToNot(HaveOccurred())
			val, err := database.Get(testHeaderKey)
			Expect(err).ToNot(HaveOccurred())
			Expect(val).To(Equal(testHeaderValue))
		})
	})

	Describe("Delete", func() {
		It("removes the key-value pair from the database", func() {
			err = database.Put(testHeaderKey, testHeaderValue)
			Expect(err).ToNot(HaveOccurred())
			val, err := database.Get(testHeaderKey)
			Expect(err).ToNot(HaveOccurred())
			Expect(val).To(Equal(testHeaderValue))

			err = database.Delete(testHeaderKey)
			Expect(err).ToNot(HaveOccurred())
			_, err = database.Get(testHeaderKey)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("sql: no rows in result set"))
		})
	})
})

var _ = Describe("Database with CID tables check", func() {
	BeforeEach(func() {
		db, err = pgipfsethdb.TestDB()
		Expect(err).ToNot(HaveOccurred())

		cacheConfig := pgipfsethdb.CacheConfig{
			Name:           "db",
			Size:           3000000, // 3MB
			ExpiryDuration: time.Hour,
		}

		database = pgipfsethdb.NewDatabase(db, cacheConfig, true)

		databaseWithBlock, ok := database.(*pgipfsethdb.Database)
		Expect(ok).To(BeTrue())
		(*databaseWithBlock).BlockNumber = testBlockNumber

		_, err = db.Exec("INSERT into public.blocks (key, data, block_number) VALUES ($1, $2, $3)",
			testAccountMhKey, testAccountLeafNode, testBlockNumber.Uint64())
		Expect(err).ToNot(HaveOccurred())
		_, err = db.Exec("INSERT into public.blocks (key, data, block_number) VALUES ($1, $2, $3)",
			testStorageMhKey, testStorageLeafNode, testBlockNumber.Uint64())
		Expect(err).ToNot(HaveOccurred())
	})
	AfterEach(func() {
		groupcache.DeregisterGroup("db")
		err = pgipfsethdb.ResetTestDB(db)
		Expect(err).ToNot(HaveOccurred())
	})

	It("throws an err if no CID entry exists in the db", func() {
		_, err = database.Get(testAccountKey)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("state_cids"))
	})
	It("returns the value associated with the key, if the state_cids entry exists", func() {
		err = insertStateCID(db)
		Expect(err).ToNot(HaveOccurred())
		val, err := database.Get(testAccountKey)
		Expect(err).ToNot(HaveOccurred())
		Expect(val).To(Equal(testAccountLeafNode))
	})
	It("returns the value associated with the key, if the storage_cids entry exists", func() {
		err = insertStorageCID(db)
		Expect(err).ToNot(HaveOccurred())
		val, err := database.Get(testStorageKey)
		Expect(err).ToNot(HaveOccurred())
		Expect(val).To(Equal(testStorageLeafNode))
	})
})

func insertStateCID(db *sqlx.DB) error {
	_, err = db.Exec("INSERT INTO eth.state_cids (block_number, header_id, state_leaf_key, cid, state_path, node_type, diff, mh_key) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		testBlockNumber.Uint64(),
		testHeaderID,
		testAccountLeafKey.String(),
		testAccountCID.String(),
		testAccountPath,
		sdtypes.Leaf.Int(),
		false,
		testAccountMhKey,
	)
	return err
}

func insertStorageCID(db *sqlx.DB) error {
	_, err = db.Exec("INSERT INTO eth.storage_cids (block_number, header_id, state_path, storage_leaf_key, cid, storage_path, node_type, diff, mh_key) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
		testBlockNumber.Uint64(),
		testHeaderID,
		testStorageStatePath,
		testStorageLeafKey.String(),
		testStorageCID.String(),
		testStoragePartialPath,
		sdtypes.Leaf.Int(),
		false,
		testStorageMhKey,
	)
	return err
}
