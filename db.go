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

	SQL_DATA_TABLE_ON_DUPLICATE_KEY_UPDATE = `
	  ON DUPLICATE KEY UPDATE
		    TIME_TYPE = VALUES(TIME_TYPE),
				FIX_TIME = VALUES(FIX_TIME)
	`

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

	SQL_DATA_SUMMARY = `
		SELECT FIX_TIME FROM %s
		WHERE TIME_TYPE = ?
		ORDER BY FIX_TIME ASC
	`

	SQL_DATA = `
		WITH PARAM AS (
				-- パラメータ
			SELECT
				? as LOWER_TIME_TYPE
				, ? AS LOWER_FIX_TIME
				, ? AS UPPER_TIME_TYPE
		), LOWER_LATEST AS (
				-- 下位足の最新ローソク足
			SELECT
					D.TIME_TYPE
					, D.FIX_TIME
					, D.CLOSE_PRICE
			FROM %s D
			LEFT JOIN PARAM P
				ON 1 = 1
			WHERE 1 = 1
				AND TIME_TYPE = P.LOWER_TIME_TYPE
				AND FIX_TIME = P.LOWER_FIX_TIME
		), UPPER_CANDLES AS (
				-- 上位足の過去のローソク足一覧
			SELECT
					D.TIME_TYPE
					, D.FIX_TIME
					, D.HIGH_PRICE
					, D.OPEN_PRICE
					, D.CLOSE_PRICE
					, D.LOW_PRICE
			FROM %s D
			LEFT JOIN PARAM P
				ON 1 = 1
			WHERE 1 = 1
				AND D.TIME_TYPE = P.UPPER_TIME_TYPE
				AND D.FIX_TIME < ALL(SELECT FIX_TIME FROM LOWER_LATEST)
			ORDER BY D.FIX_TIME DESC
			LIMIT ?
		), UPPER_LATEST AS (
				-- 上位の最新ローソク足
				SELECT
						MAX(FIX_TIME) AS FIX_TIME
				FROM UPPER_CANDLES
				LEFT JOIN PARAM P
					ON 1 = 1
				WHERE 1 = 1
					AND TIME_TYPE = P.UPPER_TIME_TYPE
		), UPPER_UNFIXED AS (
				-- 上位足の未確定足
				SELECT
					TIME_TYPE
					, D.FIX_TIME
					, D.HIGH_PRICE
				, D.LOW_PRICE
				, D.OPEN_PRICE
				, D.CLOSE_PRICE
				FROM %s D
				LEFT JOIN PARAM P
					ON 1 = 1
				WHERE 1 = 1
						AND (SELECT FIX_TIME FROM UPPER_LATEST) < D.FIX_TIME
						AND D.FIX_TIME <= (SELECT FIX_TIME FROM LOWER_LATEST)
						AND D.TIME_TYPE = P.LOWER_TIME_TYPE
		)
		SELECT
				LL.FIX_TIME,
				HIGH_AND_LOW_D.HIGH_PRICE,
				OPEN_D.OPEN_PRICE,
				LL.CLOSE_PRICE,
				HIGH_AND_LOW_D.LOW_PRICE
		FROM LOWER_LATEST LL
		LEFT JOIN (
				SELECT
						MAX(HIGH_PRICE) AS HIGH_PRICE
						, MIN(LOW_PRICE) AS LOW_PRICE
				FROM UPPER_UNFIXED D
		) HIGH_AND_LOW_D
			ON 1 = 1
		LEFT JOIN (
				SELECT
						OPEN_PRICE
				FROM UPPER_UNFIXED
				ORDER BY FIX_TIME DESC
				LIMIT 1
		) OPEN_D
			ON 1 = 1
		UNION ALL
		SELECT
			FIX_TIME
			, HIGH_PRICE
			, OPEN_PRICE
			, CLOSE_PRICE
			, LOW_PRICE
		FROM UPPER_CANDLES
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
	defer res.Close()

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

// getUploadedPairNames データがアップロードされている通貨ペア名の一覧を返却する
func (db *db) getUploadedPairNames() ([]string, error) {
	res, err := db.impl.Query(SQL_QUERY_UPLOADED_PAIR_NAMES)
	if err != nil {
		return nil, err
	}
	defer res.Close()

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
	defer res.Close()

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

	return nil
}

func (db *db) queryDataSummary(pairName string, timeType TimeType) ([]string, error) {
	sql := fmt.Sprintf(SQL_DATA_SUMMARY, pairName)
	stmt, err := db.impl.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	row, err := stmt.Query(int(timeType))
	if err != nil {
		return nil, err
	}
	defer row.Close()

	fixTimes := make([]string, 0)
	for row.Next() {
		var fixTime string
		err = row.Scan(&fixTime)
		if err != nil {
			return nil, err
		}
		fixTimes = append(fixTimes, fixTime)
	}

	return fixTimes, nil
}

func (db *db) queryData(
	pairName string,
	lowerTimeType TimeType,
	lowerFixTime string,
	upperTimeType TimeType,
	limit int) ([]Candle, error) {

	sql := fmt.Sprintf(SQL_DATA, pairName, pairName, pairName)

	stmt, err := db.impl.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query(int(lowerTimeType), lowerFixTime, int(upperTimeType), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	candles := make([]Candle, 0)
	for rows.Next() {
		var c Candle
		err := rows.Scan(&c.Time, &c.High, &c.Open, &c.Close, &c.Low)
		if err != nil {
			return nil, err
		}
		candles = append(candles, c)
	}

	if len(candles) < 2 || candles[0].Time != lowerFixTime {
		return nil, ErrInvalidData{}
	}

	return candles, nil
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
