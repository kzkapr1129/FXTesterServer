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
)

func (ErrCannotGetMaxAllowedPacket) Error() string {
	return "max_allowed_packetの取得に失敗しました"
}

func (ErrInvalidDateTimeFormat) Error() string {
	return "予期しない日付フォーマットが指定されました"
}

func (ErrInvalidTimeType) Error() string {
	return "不正な時間軸が指定されました"
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
