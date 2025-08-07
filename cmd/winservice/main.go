package main

import (
	"fmt"
	"log/slog"
	"os"
	"pgm/pkg/file_service"
	"pgm/pkg/file_service/config"
	"pgm/pkg/file_service/repo"

	win_service "github.com/YusufSert/win-service"
	"golang.org/x/sys/windows/svc"
)

// note: can we do network lvl mutex to run two file_service service. atomic access to files.
func main() {
	cfg, err := config.ReadPGMConfig("C:/Users/TCYSERT/Desktop/file_service/config")
	if err != nil {
		slog.Error(fmt.Errorf("file_service: error reading config file %w", err).Error())
		return
	}

	logFile, err := os.OpenFile(cfg.LogFilePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	logger := slog.New(slog.NewJSONHandler(logFile, &slog.HandlerOptions{Level: cfg.LogLevel, AddSource: true}))

	r, err := repo.NewPGMRepo(cfg.DBConnStr)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	s, err := file_service.NewPGMService(cfg, r, logger)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	w, err := win_service.NewWinService(s, "file_service")
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	err = svc.Run("file_service", w)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	os.Exit(0)
}

//todo: check behavior of close(channel) with select statement.
