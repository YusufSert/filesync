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
	"pgm/pkg/file_service/server"
)

const host = "0.0.0.0:9096"

// TODO: write better error message make them more descriptive
// TODO: default options and config vars
func main() {
	server := server.New(host)
	server.Run()

	path := flag.String("config.path", "", "config path for pgm")
	flag.Parse()

	cfg, err := config.ReadPGMConfig(*path)
	if err != nil {
		slog.Error(fmt.Errorf("filesync: error reading config file %w", err).Error())
		return
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel, AddSource: true}))

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
