package main

import (
	"database/sql"
	"log"
)

type action struct{}

var Action = action{}

func (action) registerData(db *db, pairName string, timeType TimeType, candles []Candle) error {
	err := db.createDataTable(pairName)
	if err != nil {
		log.Println(err)
		return err
	}

	err = db.createHeadTable(pairName)
	if err != nil {
		log.Println(err)
		return err
	}

	return db.begin(func(tx *sql.Tx) error {
		err = db.deleteHeadTable(tx, pairName, timeType)
		if err != nil {
			return err
		}

		err = db.registerData(tx, pairName, timeType, candles)
		if err != nil {
			return err
		}

		return db.registerHead(tx, pairName, timeType)
	})
}
