package main

type (
	ErrCannotGetMaxAllowedPacket struct{}
	ErrInvalidDateTimeFormat     struct{}
	ErrInvalidTimeType           struct{}
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
