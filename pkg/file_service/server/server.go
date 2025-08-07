package server

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	host string
}

func New(host string) *Server {
	s := Server{host: host}
	return &s
}

// TODO: add mertics field to config_file to expose metrics or not
func (s *Server) Run() error {
	http.Handle("GET /metrics", promhttp.Handler())
	return http.ListenAndServe(s.host, nil)
}
