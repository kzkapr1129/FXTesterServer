package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
)

type (
	ApiResponseStatus struct {
		ErrorCode    uint16 `json:"ErrorCode"`
		ErrorMessage string `json:"ErrorMessage"`
	}

	ApiResponseUpload struct {
		Status ApiResponseStatus `json:"Status"`
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
	timeType, err := getTimeType(timeTypeName)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeResponse(err)
		return
	}

	pairName := r.Header.Get("x-pair-name")
	err = checkPairName(pairName)
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
