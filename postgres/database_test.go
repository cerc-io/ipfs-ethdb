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

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/jmoiron/sqlx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	pgipfsethdb "github.com/vulcanize/ipfs-ethdb/postgres"
)

var (
	database     ethdb.Database
	db           *sqlx.DB
	err          error
	testHeader   = types.Header{Number: big.NewInt(1337)}
	testValue, _ = rlp.EncodeToBytes(testHeader)
	testEthKey   = testHeader.Hash().Bytes()
	testMhKey, _ = pgipfsethdb.MultihashKeyFromKeccak256(testEthKey)
)

var _ = Describe("Database", func() {
	BeforeEach(func() {
		db, err = pgipfsethdb.TestDB()
		Expect(err).ToNot(HaveOccurred())
		database = pgipfsethdb.NewDatabase(db)
	})
	AfterEach(func() {
		err = pgipfsethdb.ResetTestDB(db)
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("Has", func() {
		It("returns false if a key-pair doesn't exist in the db", func() {
			has, err := database.Has(testEthKey)
			Expect(err).ToNot(HaveOccurred())
			Expect(has).ToNot(BeTrue())
		})
		It("returns true if a key-pair exists in the db", func() {
			_, err = db.Exec("INSERT into public.blocks (key, data) VALUES ($1, $2)", testMhKey, testValue)
			Expect(err).ToNot(HaveOccurred())
			_, err = db.Exec("INSERT into eth.key_preimages (eth_key, ipfs_key) VALUES ($1, $2)", testEthKey, testMhKey)
			Expect(err).ToNot(HaveOccurred())
			has, err := database.Has(testEthKey)
			Expect(err).ToNot(HaveOccurred())
			Expect(has).To(BeTrue())
		})
	})

	Describe("Get", func() {
		It("throws an err if the key-pair doesn't exist in the db", func() {
			_, err = database.Get(testEthKey)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("sql: no rows in result set"))
		})
		It("returns the value associated with the key, if the pair exists", func() {
			_, err = db.Exec("INSERT into public.blocks (key, data) VALUES ($1, $2)", testMhKey, testValue)
			Expect(err).ToNot(HaveOccurred())
			_, err = db.Exec("INSERT into eth.key_preimages (eth_key, ipfs_key) VALUES ($1, $2)", testEthKey, testMhKey)
			Expect(err).ToNot(HaveOccurred())
			val, err := database.Get(testEthKey)
			Expect(err).ToNot(HaveOccurred())
			Expect(val).To(Equal(testValue))
		})
	})

	Describe("Put", func() {
		It("persists the key-value pair in the database", func() {
			_, err = database.Get(testEthKey)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("sql: no rows in result set"))

			err = database.Put(testEthKey, testValue)
			Expect(err).ToNot(HaveOccurred())
			val, err := database.Get(testEthKey)
			Expect(err).ToNot(HaveOccurred())
			Expect(val).To(Equal(testValue))
		})
	})

	Describe("Delete", func() {
		It("removes the key-value pair from the database", func() {
			err = database.Put(testEthKey, testValue)
			Expect(err).ToNot(HaveOccurred())
			val, err := database.Get(testEthKey)
			Expect(err).ToNot(HaveOccurred())
			Expect(val).To(Equal(testValue))

			err = database.Delete(testEthKey)
			Expect(err).ToNot(HaveOccurred())
			_, err = database.Get(testEthKey)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("sql: no rows in result set"))
		})
	})
})
