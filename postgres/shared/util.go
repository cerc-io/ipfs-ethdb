// VulcanizeDB
// Copyright © 2023 Vulcanize

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

package shared

import (
	"github.com/jmoiron/sqlx"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
)

var (
	testDBConfig, _ = postgres.Config{
		Hostname:     "localhost",
		DatabaseName: "cerc_testing",
		Username:     "vdbm",
		Password:     "password",
		Port:         8077,
		Driver:       "SQLX",
	}.WithEnv()
)

// TestDB connect to the testing database
// it assumes the database has the IPFS ipld.blocks table present
// DO NOT use a production db for the test db, as it will remove all contents of the ipld.blocks table
func TestDB() (*sqlx.DB, error) {
	return sqlx.Connect("postgres", testDBConfig.DbConnectionString())
}

// ResetTestDB drops all rows in the test db ipld.blocks table
func ResetTestDB(db *sqlx.DB) error {
	_, err := db.Exec("TRUNCATE ipld.blocks CASCADE")
	return err
}
