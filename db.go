package main

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

const (
	SQL_CREATE_DATA_TABLE = `
		CREATE TABLE IF NOT EXISTS %s (
			TIME_TYPE DECIMAL(1, 0),
			FIX_TIME DATETIME,
			HIGH_PRICE DECIMAL(8, 5),
			OPEN_PRICE DECIMAL(8, 5),
			CLOSE_PRICE DECIMAL(8, 5),
			LOW_PRICE DECIMAL(8, 5),
			PRIMARY KEY(TIME_TYPE,FIX_TIME)
		)
	`

	SQL_CREATE_HEAD_TABLE = `
		CREATE TABLE IF NOT EXISTS %s_HEAD (
				ID DECIMAL(6, 0),
				TIME_TYPE DECIMAL(1, 0),
				FIX_TIME DATETIME,
				PRIMARY KEY(ID,TIME_TYPE)
		)`

	SQL_INSERT_DATA = `
		INSERT INTO %s (
			TIME_TYPE,
			FIX_TIME,
			HIGH_PRICE,
			OPEN_PRICE,
			CLOSE_PRICE,
			LOW_PRICE
		) VALUES
	`

	SQL_INSERT_HEAD_TABLE = `
		INSERT INTO %s_HEAD
		SELECT
			ROW_NUMBER() OVER (PARTITION BY TIME_TYPE ORDER BY FIX_TIME ASC) AS ID,
			TIME_TYPE,
			FIX_TIME
		FROM %s
		WHERE TIME_TYPE = %d
	`

	SQL_DATA_TABLE_ON_DUPLICATE_KEY_UPDATE = `
	  ON DUPLICATE KEY UPDATE
		    TIME_TYPE = VALUES(TIME_TYPE),
				FIX_TIME = VALUES(FIX_TIME)
	`

	SQL_DELETE_HEAD_TABLE = "DELETE FROM %s_HEAD WHERE TIME_TYPE = %d"

	SQL_QUERY_UPLOADED_PAIR_NAMES = `
		SELECT TABLE_NAME FROM information_schema.tables
		WHERE 1 = 1
			AND TABLE_SCHEMA = 'fx_tester_db'
			AND TABLE_NAME REGEXP '^[A-Z]{6}$'
	`

	SQL_QUERY_UPLOADED_PAIR_DETAIL = `
		SELECT 
			TIME_TYPE
			, COUNT(*) AS NUM_DATA
		FROM %s
		GROUP BY TIME_TYPE
	`

	SQL_DELETE_DATA = `
			DELETE FROM %s WHERE TIME_TYPE in (%s)
	`

	SQL_DELETE_HEAD = `
		DELETE FROM %s_HEAD WHERE TIME_TYPE in (%s)
	`
)

type (
	TrashScanner struct{}

	db struct {
		config           *config
		impl             *sql.DB
		maxAllowedPacket int
	}
)

func (TrashScanner) Scan(interface{}) error {
	return nil
}

// newDB DBクラスのnewする
func newDB(config *config) *db {
	return &db{config: config}
}

// open DBを開く
func (db *db) open() error {
	db.close()

	// 接続情報の作成
	conInfo := fmt.Sprintf("%s:%s@(%s:%d)/%s",
		db.config.DBUserName,
		db.config.DBUserPass,
		db.config.DBAddress,
		db.config.DBPort,
		db.config.DatabaseName)

	// データベースを開く
	impl, err := sql.Open("mysql", conInfo)
	if err != nil {
		return err
	}

	// DBの疎通確認
	err = impl.Ping()
	if err != nil {
		impl.Close()
		return err
	}

	// バルクインサートの最大数を取得
	res, err := impl.Query("show variables like 'max_allowed_packet'")
	if err != nil {
		return err
	}
	if !res.Next() {
		return ErrCannotGetMaxAllowedPacket{}
	}
	var maxAllowedPacket int
	err = res.Scan(TrashScanner{}, &maxAllowedPacket)
	if err != nil {
		return err
	}

	// バルクインサートの最大数の不正値チェック
	if maxAllowedPacket <= 0 {
		return ErrCannotGetMaxAllowedPacket{}
	}

	db.impl = impl
	db.maxAllowedPacket = maxAllowedPacket

	return nil
}

// close DBをクローズする
func (db *db) close() error {
	if db.impl == nil {
		return nil
	}

	err := db.impl.Close()
	db.impl = nil
	return err
}

// begin トランザクションを開始する
func (db *db) begin(transaction func(tx *sql.Tx) error) error {
	log.Println("transaction started..")
	tx, err := db.impl.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if res := recover(); res != nil {
			tx.Rollback()
			log.Println("transaction failed... ", res)
		} else if err != nil {
			tx.Rollback()
			log.Println("transaction failed... ", err)
		} else {
			tx.Commit()
			log.Println("transaction successed!!")
		}
	}()

	err = transaction(tx)
	return err
}

// createDataTable データテーブルを作成する
func (db *db) createDataTable(pairName string) error {
	sql := fmt.Sprintf(SQL_CREATE_DATA_TABLE, pairName)
	_, err := db.impl.Exec(sql)
	return err
}

// createHeadTable ヘッドテーブルを作成する
func (db *db) createHeadTable(pairName string) error {
	sql := fmt.Sprintf(SQL_CREATE_HEAD_TABLE, pairName)
	_, err := db.impl.Exec(sql)
	return err
}

// deleteHeadTable ヘッドテーブルを全削除します
func (db *db) deleteHeadTable(tx *sql.Tx, pairName string, timeType TimeType) error {
	sql := fmt.Sprintf(SQL_DELETE_HEAD_TABLE, pairName, int(timeType))
	_, err := tx.Exec(sql)
	return err
}

// registerData データテーブルにデータを挿入する
func (db *db) registerData(tx *sql.Tx, pairName string, timeType TimeType, candles []Candle) error {
	if timeType == Unknown {
		return ErrInvalidTimeType{}
	}

	for i := 0; ; i += db.maxAllowedPacket {
		numInsert := Utils.minInt(db.maxAllowedPacket, len(candles)-i)
		if numInsert <= 0 {
			break
		}

		slice := candles[i : i+numInsert]
		sql, err := makeInsertDataSql(pairName, timeType, slice)
		if err != nil {
			return err
		}

		_, err = tx.Exec(sql)
		if err != nil {
			return err
		}
	}

	return nil
}

// registerHead ヘッドテーブルにデータを挿入する
func (db *db) registerHead(tx *sql.Tx, pairName string, timeType TimeType) error {
	sql := fmt.Sprintf(SQL_INSERT_HEAD_TABLE, pairName, pairName, int(timeType))
	_, err := tx.Exec(sql)
	return err
}

// getUploadedPairNames データがアップロードされている通貨ペア名の一覧を返却する
func (db *db) getUploadedPairNames() ([]string, error) {
	res, err := db.impl.Query(SQL_QUERY_UPLOADED_PAIR_NAMES)
	if err != nil {
		return nil, err
	}

	pairNames := make([]string, 0)
	for res.Next() {
		var pairName string
		err = res.Scan(&pairName)
		if err != nil {
			return nil, err
		}

		pairNames = append(pairNames, pairName)
	}

	return pairNames, nil
}

func (db *db) getUploadedPairDetail(pairName string) (map[int]int, error) {
	sql := fmt.Sprintf(SQL_QUERY_UPLOADED_PAIR_DETAIL, pairName)
	res, err := db.impl.Query(sql)
	if err != nil {
		return nil, err
	}

	countTable := make(map[int]int)
	for res.Next() {
		var timeType int
		var countData int
		res.Scan(&timeType, &countData)

		countTable[timeType] = countData
	}

	return countTable, nil
}

func (db *db) deleteData(tx *sql.Tx, pairName string, timeTypes []TimeType) error {

	inStatement := strings.Join(mapArray(timeTypes, func(v TimeType) string {
		return strconv.FormatInt(int64(v), 10)
	}), ",")

	deleteDataSql := fmt.Sprintf(SQL_DELETE_DATA, pairName, inStatement)
	_, err := tx.Exec(deleteDataSql)
	if err != nil {
		return err
	}

	deleteHeadSql := fmt.Sprintf(SQL_DELETE_HEAD, pairName, inStatement)
	_, err = tx.Exec(deleteHeadSql)
	return err
}

// makeInsertDataSql データテーブルへの挿入用SQLを作成し返却する
func makeInsertDataSql(pairName string, timeType TimeType, candles []Candle) (string, error) {
	sqlBase := fmt.Sprintf(SQL_INSERT_DATA, pairName)

	valueStatements := []string{}

	for k := 0; k < len(candles); k++ {

		c := candles[k]
		t, err := Utils.getCandleFixTime(c.Time, timeType)
		if err != nil {
			return "", err
		}

		valueStatement := fmt.Sprintf(
			"(%d, '%s', %f, %f, %f, %f)",
			int(timeType),
			t, c.High, c.Open, c.Close, c.Low)

		valueStatements = append(valueStatements, valueStatement)
	}

	return sqlBase + strings.Join(valueStatements, ",") + SQL_DATA_TABLE_ON_DUPLICATE_KEY_UPDATE, nil
}
