package server

import (
	"fmt"
	"github.com/lostz/logging"
	"github.com/lostz/straw/config"
)

var logger = logging.GetLogger("server")

type Server struct {
	config *config.Config
}

func (self *Server) Start() error {
	_, err := self.writeBinglogDumpCommand()
	if err != nil {
		return err
	}
	return nil

}

func (self *Server) writeBinglogDumpCommand() (*Conn, error) {
	conn := &Conn{}
	err := conn.Connect(self.config.MasterAddr, self.config.Username, self.config.Password, "", self.config.Host, self.config.Port, self.config.ServerId, self.config.MasterId)
	if err != nil {
		logger.Errorf("connet addr %s", self.config.MasterAddr, err.Error())
		return conn, err
	}
	if err = conn.RegistSlave(); err != nil {
		logger.Errorf("registe slave %s", err.Error())
		return conn, err
	}
	if _, err = conn.Execute("SET @mariadb_slave_capability = 4"); err != nil {
		logger.Errorf("execute SET @mariadb_slave_capability = 4 %s ", err.Error())
		return conn, err
	}
	query := fmt.Sprintf("SET @slave_connect_state='%s'", self.config.GtidSlavePos)
	if _, err = conn.Execute(query); err != nil {
		logger.Errorf("execute %s %s", query, err.Error())
		return conn, err
	}
	if _, err := conn.Execute("SET @slave_gtid_strict_mode=1"); err != nil {
		logger.Errorf("execued set @slave_gtid_strict_mode=1: %s", err.Error())
		return conn, err
	}

	return conn, nil

}
