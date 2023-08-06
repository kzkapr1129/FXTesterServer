package main

import (
	"fmt"
	"strings"
)

type (
	ErrCannotGetMaxAllowedPacket struct{}
	ErrInvalidDateTimeFormat     struct{}
	ErrInvalidTimeType           struct{}
	ErrMultipleCause             struct{ errors []error }
	ErrInvalidPairName           struct{}
	ErrEmptyCandles              struct{}
)

func getErrorStatus(err error) (uint16, string) {
	if err == nil {
		return 0, "OK"
	}

	if _, ok := err.(ErrCannotGetMaxAllowedPacket); ok {
		return 0x8001, err.Error()
	}

	if _, ok := err.(ErrInvalidDateTimeFormat); ok {
		return 0x8002, err.Error()
	}

	if _, ok := err.(ErrInvalidTimeType); ok {
		return 0x8003, err.Error()
	}

	if _, ok := err.(ErrMultipleCause); ok {
		return 0x8004, err.Error()
	}

	if _, ok := err.(ErrInvalidPairName); ok {
		return 0x8005, err.Error()
	}

	return 0x8FFF, err.Error()
}

func (ErrCannotGetMaxAllowedPacket) Error() string {
	return "max_allowed_packetの取得に失敗しました"
}

func (ErrInvalidDateTimeFormat) Error() string {
	return "予期しない日付フォーマットが指定されました"
}

func (ErrInvalidTimeType) Error() string {
	return "不正な時間軸が指定されました"
}

func (ErrEmptyCandles) Error() string {
	return "ローソク足は必ず1件以上指定してください"
}

func (e ErrMultipleCause) Error() string {
	switch len(e.errors) {
	case 0:
		return ""
	case 1:
		{
			if e.errors[0] != nil {
				return e.errors[0].Error()
			}
			return ""
		}
	default:
		errors := make([]string, 0)
		for _, err := range e.errors {
			if err != nil {
				errors = append(errors, err.Error())
			}
		}
		return fmt.Sprintf("複数のエラーが発生しました: %s", strings.Join(errors, ","))
	}
}

func newErrMultipleCause(arguments ...error) error {
	errors := make([]error, 0)
	for _, err := range arguments {
		errors = append(errors, err)
	}
	return ErrMultipleCause{errors: errors}
}

func (ErrInvalidPairName) Error() string {
	return "通貨ペア名が不正です。通貨ペア名に使用できる文字は大文字の英字で6文字までです"
}
