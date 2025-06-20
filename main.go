package main

import (
	"fmt"
	"log"
	"pgm/app"
	"pgm/repo"
	"pgm/tools/logger"

	win_service "github.com/YusufSert/win-service"
	"golang.org/x/sys/windows/svc"
)

// note: can we do network lvl mutex to run two pgm service. atomic access to files.
func main() {
	cfg, err := app.ReadPGMConfig("C:/Users/TCYSERT/Desktop/pgm/config")
	if err != nil {
		log.Fatal(fmt.Errorf("pgm: error reading config file %w", err))
		return
	}
	l, err := logger.NewLogger(cfg.LogFilePath) //todo: options pattern maybe for path and loglevel // what is inifunction in viper
	if err != nil {
		log.Fatal(err)
		return
	}
	defer l.Close() // close the log file when main exits
	l.SetLevel(cfg.LogLevel)

	r, err := repo.NewPGMRepo(cfg.DBConnStr)
	if err != nil {
		l.Logger.Error(err.Error())
		return
	}

	pgm, err := app.NewPGMService(cfg, r, l.Logger)
	if err != nil {
		l.Logger.Error(err.Error())
		return
	}

	w, err := win_service.NewWinService(pgm, "pgm")
	if err != nil {
		l.Logger.Error(err.Error())
		return
	}

	err = svc.Run("pgm", w)
	if err != nil {
		l.Logger.Error(err.Error())
	}
}

//todo: check behavior of close(channel) with select statement.
