// Package main ここにパッケージの説明を書きます。
package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"
)

// Config hoge
type config struct {
	DBUserName   string
	DBUserPass   string
	DBAddress    string
	DBPort       int
	DatabaseName string
	ServerPort   int
}

// loadConfig　設定ファイルの読み込み
func loadConfig() (*config, error) {
	f, err := os.Open("config.json")
	if err != nil {
		return nil, err
	}
	config := config{}
	err = json.NewDecoder(f).Decode(&config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

// main ホゲホゲ
func main() {

	log.Println("load config...")
	config, err := loadConfig()
	if err != nil {
		log.Println("Failed to load config.json", err)
		return
	}

	db := newDB(config)
	defer db.close()

	err = db.open()
	if err != nil {
		log.Println(err)
		return
	}

	err = db.createDataTable("EURUSD")
	if err != nil {
		log.Println(err)
		return
	}

	err = db.createHeadTable("EURUSD")
	if err != nil {
		log.Println(err)
		return
	}

	err = db.truncateHeadTable("EURUSD")
	if err != nil {
		log.Println(err)
		return
	}

	db.begin(func(tx *sql.Tx) error {
		candles := make([]Candle, 7)
		candles[0].Time = "2023.03.30 20:00"
		candles[1].Time = "2023.03.30 21:00"
		candles[2].Time = "2023.03.30 22:00"
		candles[3].Time = "2023.03.30 23:00"
		candles[4].Time = "2023.03.31 00:00"
		candles[5].Time = "2023.03.31 01:00"
		candles[6].Time = "2023.03.31 02:00"
		err = db.registerData(tx, "EURUSD", timeTypeOf("H1"), candles)
		if err != nil {
			return err
		}

		return db.registerHead(tx, "EURUSD")
	})
}
