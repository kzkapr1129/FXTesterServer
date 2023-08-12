package main

import "time"

type (
	// TimeType 時間軸を示す型です
	TimeType int

	// Pair 通貨ペア名を示す型です
	Pair int

	Candle struct {
		Time       string  `json:"time"`
		High       float32 `json:"high"`
		Open       float32 `json:"open"`
		Close      float32 `json:"close"`
		Low        float32 `json:"low"`
		TickVolume int32   `json:"tickVolume"`
	}

	UploadPayload struct {
		Data []Candle `json:"data"`
	}
)

const (
	M1 TimeType = iota
	M5
	M15
	M30
	H1
	H4
	Daily
	Weekly
	Unknown
	NumTimeType = Unknown
)

// timeTypeOf は文字列を[timeType]型に変換します
func timeTypeOf(value string) TimeType {
	switch value {
	case "M1":
		return M1
	case "M5":
		return M5
	case "M15":
		return M15
	case "M30":
		return M30
	case "H1":
		return H1
	case "H4":
		return H4
	case "Daily":
		return Daily
	case "Weekly":
		return Weekly
	}
	return Unknown
}

func (t TimeType) getDuration() (time.Duration, error) {
	switch t {
	case M1:
		return time.Duration(1 * time.Minute), nil
	case M5:
		return time.Duration(5 * time.Minute), nil
	case M15:
		return time.Duration(15 * time.Minute), nil
	case M30:
		return time.Duration(30 * time.Minute), nil
	case H1:
		return time.Duration(1 * time.Hour), nil
	case H4:
		return time.Duration(4 * time.Hour), nil
	case Daily:
		return time.Duration(1 * 24 * time.Hour), nil
	case Weekly:
		return time.Duration(7 * 24 * time.Hour), nil
	}

	return time.Duration(0 * time.Second), ErrInvalidTimeType{}
}

func (t TimeType) toInt() int {
	return int(t)
}
