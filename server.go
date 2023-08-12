package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
)

type (
	ApiResponseStatus struct {
		ErrorCode    uint16 `json:"code"`
		ErrorMessage string `json:"message"`
	}

	ApiResponseUpload struct {
		Status ApiResponseStatus `json:"status"`
	}

	ApiResponseGetUploadedPairs struct {
		Status    ApiResponseStatus `json:"status"`
		PairNames []string          `json:"pairs"`
	}

	PairDetail struct {
		TimeType  int `json:"timeType"`
		CountData int `json:"countData"`
	}

	ApiResponseGetUploadedPairDetail struct {
		Status      ApiResponseStatus `json:"status"`
		PairDetails []PairDetail      `json:"details"`
	}

	ApiResponseDeleteData struct {
		Status ApiResponseStatus `json:"status"`
	}
)

func newApiResponseStatus(err error) ApiResponseStatus {
	errorCode, errorMessage := getErrorStatus(err)
	return ApiResponseStatus{ErrorCode: errorCode, ErrorMessage: errorMessage}
}

type server struct {
	impl *http.Server
	db   *db
}

func newServer(c *config) (*server, error) {
	log.Printf("use port: %d\n", c.ServerPort)

	db := newDB(c)
	err := db.open()
	if err != nil {
		return nil, err
	}

	return &server{
		impl: &http.Server{Addr: fmt.Sprintf(":%d", c.ServerPort)},
		db:   db,
	}, nil
}

func (s *server) accept() error {
	http.HandleFunc("/api/upload", s.handleUpload)
	http.HandleFunc("/api/uploaded_pairs", s.handleGetUploadedPairs)
	http.HandleFunc("/api/uploaded_pair_detail", s.handleGetUploadedPairDetail)
	http.HandleFunc("/api/delete_data", s.handleDeleteData)

	err := s.impl.ListenAndServe()
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (s *server) shutdown(ctx context.Context) error {
	if s.impl == nil {
		return nil
	}

	log.Println("サーバーをシャットダウン中です")
	errShutdown := s.impl.Shutdown(ctx)

	if s.db == nil {
		return errShutdown
	}

	log.Println("データベースをクローズ中です")
	errDbClose := s.db.close()
	if errDbClose != nil {
		return newErrMultipleCause(errShutdown, errDbClose)
	}

	log.Println("サーバーリソースの解放に成功しました")
	return nil
}

func (s *server) handleUpload(w http.ResponseWriter, r *http.Request) {
	supportedParams := []string{
		"x-pair-name",
		"x-time-type",
		"Content-Type",
	}
	supportedMethods := []string{
		"POST",
		"OPTIONS",
	}

	if handleCORS(w, r, supportedParams, supportedMethods) {
		return
	}

	writeResponse := func(err error) {
		status := newApiResponseStatus(err)
		json.NewEncoder(w).Encode(ApiResponseUpload{Status: status})
	}

	timeTypeName := r.Header.Get("x-time-type")
	timeType, err := Utils.getTimeType(timeTypeName)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeResponse(err)
		return
	}

	pairName := r.Header.Get("x-pair-name")
	err = Utils.checkPairName(pairName)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeResponse(err)
		return
	}

	var payload UploadPayload
	err = json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeResponse(err)
		return
	}

	if len(payload.Data) <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		writeResponse(ErrEmptyCandles{})
		return
	}

	err = Action.registerData(s.db, pairName, timeType, payload.Data)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeResponse(err)
		return
	}

	writeResponse(nil)
}

func (s *server) handleGetUploadedPairs(w http.ResponseWriter, r *http.Request) {
	supportedParams := []string{
		"Content-Type",
	}
	supportedMethods := []string{
		"GET",
		"OPTIONS",
	}

	if handleCORS(w, r, supportedParams, supportedMethods) {
		return
	}

	writeResponse := func(err error, pairNames []string) {
		status := newApiResponseStatus(err)
		json.NewEncoder(w).Encode(ApiResponseGetUploadedPairs{Status: status, PairNames: pairNames})
	}

	pairNames, err := s.db.getUploadedPairNames()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeResponse(err, []string{})
		return
	}

	writeResponse(nil, pairNames)

}

func (s *server) handleGetUploadedPairDetail(w http.ResponseWriter, r *http.Request) {
	supportedParams := []string{
		"x-pair-name",
		"Content-Type",
	}
	supportedMethods := []string{
		"GET",
		"OPTIONS",
	}

	if handleCORS(w, r, supportedParams, supportedMethods) {
		return
	}

	writeResponse := func(err error, countTable map[int]int) {
		status := newApiResponseStatus(err)
		pairDetails := make([]PairDetail, 0)
		for timeType, countData := range countTable {
			pairDetails = append(pairDetails, PairDetail{TimeType: timeType, CountData: countData})
		}
		sort.Slice(pairDetails, func(i, j int) bool { return pairDetails[i].TimeType < pairDetails[j].TimeType })
		json.NewEncoder(w).Encode(ApiResponseGetUploadedPairDetail{Status: status, PairDetails: pairDetails})
	}

	pairName := r.Header.Get("x-pair-name")
	err := Utils.checkPairName(pairName)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeResponse(err, make(map[int]int))
		return
	}

	countTable, err := s.db.getUploadedPairDetail(pairName)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeResponse(err, make(map[int]int))
		return
	}

	writeResponse(nil, countTable)

}

func (s *server) handleDeleteData(w http.ResponseWriter, r *http.Request) {

	supportedParams := []string{
		"x-pair-name",
		"Content-Type",
	}
	timeTypePramas := make([]string, int(NumTimeType))
	for i := 0; i < int(NumTimeType); i++ {
		timeTypePramas[i] = fmt.Sprintf("x-time-type-%d", i)
	}
	supportedParams = append(supportedParams, timeTypePramas...)

	supportedMethods := []string{
		"DELETE",
		"OPTIONS",
	}

	if handleCORS(w, r, supportedParams, supportedMethods) {
		return
	}

	writeResponse := func(err error) {
		status := newApiResponseStatus(err)
		json.NewEncoder(w).Encode(ApiResponseDeleteData{Status: status})
	}

	pairName := r.Header.Get("x-pair-name")
	err := Utils.checkPairName(pairName)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeResponse(err)
		return
	}

	timeTypes := make([]TimeType, 0)
	for _, timeTypeParam := range timeTypePramas {
		timeTypeName := r.Header.Get(timeTypeParam)
		timeType, err := Utils.getTimeType(timeTypeName)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeResponse(ErrInvalidTimeType{})
			return
		}
		timeTypes = append(timeTypes, timeType)
	}

	if len(timeTypes) <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		writeResponse(ErrInvalidTimeType{})
		return
	}

	err = s.db.begin(func(tx *sql.Tx) error {
		return s.db.deleteData(tx, pairName, timeTypes)
	})

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeResponse(ErrInvalidTimeType{})
		return
	}

	writeResponse(nil)
}

func handleCORS(w http.ResponseWriter, r *http.Request,
	supportedParams []string, supportedMethods []string) bool {

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", strings.Join(supportedParams, ","))
	w.Header().Set("Access-Control-Allow-Methods", strings.Join(supportedMethods, ","))
	w.Header().Set("Content-Type", "application/json")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return true
	}

	return false
}
