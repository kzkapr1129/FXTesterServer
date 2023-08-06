package main

import (
	"fmt"
	"regexp"
	"time"
)

type utils struct{}

var Utils utils

func (utils) minInt(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

func (utils) getStringOrDefault(str string, def string) string {
	if str == "" {
		return def
	}
	return str
}

func (utils) convXMTimeToJpTime(dateTime string) (*time.Time, error) {
	rep := regexp.MustCompile(`(\d{4})\.(0[1-9]|1[0-2])\.(0[1-9]|1[0-9]|2[0-9]|3[0-1])(\s+([0-1][0-9]|2[0-3]):([0-5]\d)|\b)$`)

	group := rep.FindStringSubmatch(dateTime)
	if group == nil {
		return nil, ErrInvalidDateTimeFormat{}
	}

	year := group[1]
	month := group[2]
	day := group[3]
	hour := Utils.getStringOrDefault(group[5], "00")
	min := Utils.getStringOrDefault(group[6], "00")

	t, err := time.Parse(
		"2006-01-02 15:04:05",
		fmt.Sprintf("%s-%s-%s %s:%s:00", year, month, day, hour, min))

	if err != nil {
		return nil, ErrInvalidDateTimeFormat{}
	}

	convJpTime := func(t time.Time) time.Time {
		// XMのサーバー時間を日本時間に変換
		if isSummerTime := Utils.inSummerTime(t); isSummerTime {
			// サマータイムの場合
			jpTime := t.Add(time.Duration(6 * time.Hour))
			return jpTime
		} else {
			// サマータイム以外の場合
			jpTime := t.Add(time.Duration(7 * time.Hour))
			return jpTime
		}
	}

	// XMの時刻を日本時間を取得
	jpTime := convJpTime(t)
	return &jpTime, nil
}

func (utils) getCandleFixTime(dateTime string, timeType TimeType) (string, error) {
	t, err := Utils.convXMTimeToJpTime(dateTime)
	if err != nil {
		return "", err
	}

	duration, err := timeType.getDuration()
	if err != nil {
		return "", err
	}

	deltaTime := t.Add(duration)

	return deltaTime.Format("2006-01-02 15:04:05"), nil
}

func (utils) inSummerTime(t time.Time) bool {
	/*
	* XMのサマータイムの仕様
	* 夏時間　3月最終の日曜日午前1時〜10月最終の日曜日午前1時 => GMT+2
	* 冬時間　10月最終の日曜日午前1時〜3月最終の日曜日午前1時 => GMT+3
	 */

	switch t.Month() {
	// 4月、5月、6月、7月、8月、9月の場合
	case 4, 5, 6, 7, 8, 9:
		return true // サマータイム

	// 10月の場合
	case 10:
		isPassedLastSunday := 31-t.Day()+int(t.Weekday())/7 == 0 // 最終日曜日、または過ぎたか
		if isPassedLastSunday {
			// 10月最終日曜日を過ぎた場合
			if t.Weekday() == time.Sunday && t.Hour() < 1 {
				// 10月最終日曜日かつ午前1時前
				return true // サマータイム中
			}
			return false // サマータイムではない
		}
		return true // サマータイム中

	// 3月の場合
	case 3:
		isPassedLastSunday := 31-t.Day()+int(t.Weekday())/7 == 0 // 最終日曜日、または過ぎたか
		if isPassedLastSunday {
			// 3月最終日曜日以降
			if t.Weekday() == time.Sunday && t.Hour() < 1 {
				// 10月最終日曜日かつ午前1時前
				return false // サマータイムではない
			}
			return true // サマータイム中
		}
		return false // サマータイムではない

	default:
		return false
	}
}

func (utils) checkPairName(pairName string) error {
	if len(pairName) != 6 {
		return ErrInvalidPairName{}
	}

	rep := regexp.MustCompile(`(?:[A-Z]{6})$`)
	if !rep.MatchString(pairName) {
		return ErrInvalidPairName{}
	}

	return nil
}

func (utils) getTimeType(timeTypeName string) (TimeType, error) {
	timeType := timeTypeOf(timeTypeName)
	if timeType == Unknown {
		return timeType, ErrInvalidTimeType{}
	}
	return timeType, nil
}
