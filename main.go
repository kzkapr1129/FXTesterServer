// Package main メインパッケージ
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"time"
)

// config 設定ファイルの内容を管理する構造体
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

// main プログラムのエントリーポイント
func main() {
	log.Println("設定ファイルを読み込んでいます")
	config, err := loadConfig()
	if err != nil {
		log.Println("Failed to load config.json", err)
		return
	}

	s, err := newServer(config)
	if err != nil {
		log.Println("Failed to initialize server", err)
		return
	}

	go func() {
		s.accept()
		log.Println("サーバーの受信待ちを終了しました")
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)

	<-quit

	log.Println("終了処理を開始します")

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	if err := s.shutdown(ctx); err != nil {
		log.Print(err)
	}

	log.Println("システムを終了します")
}
