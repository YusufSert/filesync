package config

import (
	"fmt"
	"github.com/spf13/viper"
	"log/slog"
	"strings"
	"time"
)

type PGMConfig struct {
	User, Password       string
	Addr                 string
	DBConnStr            string
	NetworkToUploadPath  string
	NetworkOutgoingPath  string
	NetworkIncomingPath  string
	NetworkDuplicatePath string
	NetworkBasePath      string
	FTPWritePath         string
	FTPReadPath          string
	PoolInterval         time.Duration
	HeartBeatInterval    time.Duration
	LogFilePath          string
	LokiPushURl          string
	LogLevel             slog.Level
}

func ReadPGMConfig(path string) (*PGMConfig, error) {
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(path)
	err := v.ReadInConfig()
	if err != nil {
		return nil, err
	}

	for _, key := range v.AllKeys() {
		if !v.IsSet(key) {
			return nil, fmt.Errorf("file_service: %s not set", key)
		}
	}

	poolInterval, err := time.ParseDuration(v.GetString("poolInterval"))
	if err != nil {
		return nil, fmt.Errorf("file_service: couldn't parse poolInterval %s", v.GetString("poolInterval"))
	}
	heartbeatInterval, err := time.ParseDuration(v.GetString("heartBeatInterval"))
	if err != nil {
		return nil, fmt.Errorf("file_service: couldn't parse heartBeatInterval %s", v.GetString("heartBeatInterval"))
	}

	logLevel := v.GetInt("logLevel")

	return &PGMConfig{
		User:                 v.GetString("user"),
		Password:             v.GetString("password"),
		Addr:                 v.GetString("addr"),
		DBConnStr:            v.GetString("dbConnStr"),
		NetworkToUploadPath:  trim(v.GetString("networkToUploadPath")),
		NetworkOutgoingPath:  v.GetString("networkOutgoingPath"),
		NetworkIncomingPath:  v.GetString("networkIncomingPath"),
		NetworkDuplicatePath: v.GetString("networkDuplicatePath"),
		NetworkBasePath:      trim(v.GetString("networkBasePath")),
		FTPWritePath:         v.GetString("ftpWritePath"),
		FTPReadPath:          v.GetString("ftpReadPath"),
		LogFilePath:          v.GetString("logFilePath"),
		LokiPushURl:          v.GetString("lokiPushURL"),
		LogLevel:             slog.Level(logLevel),
		PoolInterval:         poolInterval,
		HeartBeatInterval:    heartbeatInterval,
	}, nil
}

func trim(str string) string {
	return strings.TrimRight(strings.TrimLeft(str, " "), " ")
}
