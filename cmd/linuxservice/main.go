package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"pgm/pkg/file_service"
	"pgm/pkg/file_service/config"
	"pgm/pkg/file_service/repo"
)

// note: can we do network lvl mutex to run two file_service service. atomic access to files.
// todo: write better error message make them more descriptive
// implement default options and config vars
func main() {
	path := flag.String("config.path", "", "config path for pgm")
	flag.Parse()

	cfg, err := config.ReadPGMConfig(*path)
	if err != nil {
		slog.Error(fmt.Errorf("filesync: error reading config file %w", err).Error())
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

	err = s.Run(context.Background())
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	os.Exit(0)
}

//todo: check behavior of close(channel) with select statement.
