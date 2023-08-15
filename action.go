package main

import (
	"database/sql"
	"log"
)

type action struct{}

var Action = action{}

func (action) postData(db *db, pairName string, timeType TimeType, candles []Candle) error {
	err := db.createDataTable(pairName)
	if err != nil {
		log.Println(err)
		return err
	}

	return db.begin(func(tx *sql.Tx) error {
		return db.registerData(tx, pairName, timeType, candles)
	})
}
