package server

import (
	"github.com/lostz/straw/config"
	"testing"
)

func TestConnectDB(t *testing.T) {
	conf := &config.Config{
		Port:         6012,
		Host:         "10.88.104.18",
		MasterAddr:   "10.88.147.1:6012",
		Username:     "repl",
		Password:     "repl",
		MasterId:     14716012,
		ServerId:     14716013,
		GtidSlavePos: "0-14716012-2170",
	}
	s := &Server{config: conf}
	s.Start()

}
