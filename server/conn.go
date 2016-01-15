package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/lostz/straw/mysql"
	"net"
)

type Conn struct {
	conn       net.Conn
	pkg        *mysql.Packets
	host       string
	port       int
	addr       string
	user       string
	password   string
	db         string
	serverID   int
	masterID   int
	capability uint32
	status     uint16
	collation  mysql.CollationId
	charset    string
	salt       []byte
	pkgErr     error
}

func (self *Conn) Connect(addr, user, password, db, host string, port, serverID, masterID int) error {
	self.host = host
	self.port = port
	self.serverID = serverID
	self.addr = addr
	self.user = user
	self.password = password
	self.db = db
	//use utf8
	self.collation = mysql.DEFAULT_COLLATION_ID
	self.charset = mysql.DEFAULT_CHARSET
	return self.ReConnect()
}

func (self *Conn) ReConnect() error {
	if self.conn != nil {
		self.conn.Close()
	}

	conn, err := net.Dial("tcp", self.addr)
	if err != nil {
		return err
	}

	self.conn = conn
	self.pkg = mysql.NewPackets(conn)

	if err := self.readInitialHandshake(); err != nil {
		self.conn.Close()
		return err
	}

	if err := self.writeAuthHandshake(); err != nil {
		self.conn.Close()

		return err
	}

	if _, err := self.readOK(); err != nil {
		self.conn.Close()

		return err
	}

	//we must always use autocommit
	if !self.IsAutoCommit() {
		if _, err := self.exec("set autocommit = 1"); err != nil {
			self.conn.Close()

			return err
		}
	}

	return nil
}

func (self *Conn) Execute(command string) (*mysql.Result, error) {
	return self.exec(command)
}

func (self *Conn) RegistSlave() error {
	self.pkg.Sequence = 0
	data := make([]byte, 4+1+4+1+len(self.host)+1+len(self.user)+1+len(self.password)+2+4+4)
	pos := 4
	data[pos] = mysql.COM_REGISTER_SLAVE
	pos++

	binary.LittleEndian.PutUint32(data[pos:], uint32(self.serverID))
	pos += 4

	data[pos] = uint8(len(self.host))
	pos++
	n := copy(data[pos:], self.host)
	pos += n

	data[pos] = uint8(len(self.user))
	pos++
	n = copy(data[pos:], self.user)
	pos += n

	data[pos] = uint8(len(self.password))
	pos++
	n = copy(data[pos:], self.password)
	pos += n

	binary.LittleEndian.PutUint16(data[pos:], uint16(self.port))
	pos += 2

	//replication rank, not used
	binary.LittleEndian.PutUint32(data[pos:], 0)
	pos += 4

	binary.LittleEndian.PutUint32(data[pos:], uint32(self.masterID))

	return self.pkg.WritePacket(data)

}

func (self *Conn) readInitialHandshake() error {
	data, err := self.pkg.ReadPacket()
	if err != nil {
		return err
	}

	if data[0] == mysql.ERR_HEADER {
		return errors.New("read initial handshake error")
	}

	if data[0] < mysql.MinProtocolVersion {
		return fmt.Errorf("invalid protocol version %d, must >= 10", data[0])
	}

	pos := 1 + bytes.IndexByte(data[1:], 0x00) + 1 + 4

	self.salt = append(self.salt, data[pos:pos+8]...)

	pos += 8 + 1

	self.capability = uint32(binary.LittleEndian.Uint16(data[pos : pos+2]))

	pos += 2

	if len(data) > pos {
		pos += 1

		self.status = binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		self.capability = uint32(binary.LittleEndian.Uint16(data[pos:pos+2]))<<16 | self.capability

		pos += 2
		pos += 10 + 1
		self.salt = append(self.salt, data[pos:pos+12]...)
	}

	return nil
}

func (self *Conn) writeAuthHandshake() error {
	capability := mysql.CLIENT_PROTOCOL_41 | mysql.CLIENT_SECURE_CONNECTION |
		mysql.CLIENT_LONG_PASSWORD | mysql.CLIENT_TRANSACTIONS | mysql.CLIENT_LONG_FLAG

	capability &= self.capability

	length := 4 + 4 + 1 + 23

	length += len(self.user) + 1

	auth := mysql.CalcPassword(self.salt, []byte(self.password))

	length += 1 + len(auth)

	if len(self.db) > 0 {
		capability |= mysql.CLIENT_CONNECT_WITH_DB

		length += len(self.db) + 1
	}

	self.capability = capability

	data := make([]byte, length+4)

	data[4] = byte(capability)
	data[5] = byte(capability >> 8)
	data[6] = byte(capability >> 16)
	data[7] = byte(capability >> 24)

	data[12] = byte(self.collation)

	pos := 13 + 23

	if len(self.user) > 0 {
		pos += copy(data[pos:], self.user)
	}
	pos++

	data[pos] = byte(len(auth))
	pos += 1 + copy(data[pos+1:], auth)

	if len(self.db) > 0 {
		pos += copy(data[pos:], self.db)
	}

	return self.pkg.WritePacket(data)
}

func (self *Conn) readOK() (*mysql.Result, error) {
	data, err := self.pkg.ReadPacket()
	if err != nil {
		return nil, err
	}

	if data[0] == mysql.OK_HEADER {
		return self.handleOKPacket(data)
	} else if data[0] == mysql.ERR_HEADER {
		return nil, self.handleErrorPacket(data)
	} else {
		return nil, errors.New("invalid ok packet")
	}
}

func (self *Conn) handleOKPacket(data []byte) (*mysql.Result, error) {
	var n int
	var pos int = 1

	r := new(mysql.Result)

	r.AffectedRows, _, n = mysql.LengthEncodedInt(data[pos:])
	pos += n
	r.InsertId, _, n = mysql.LengthEncodedInt(data[pos:])
	pos += n

	if self.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		r.Status = binary.LittleEndian.Uint16(data[pos:])
		self.status = r.Status
		pos += 2
	} else if self.capability&mysql.CLIENT_TRANSACTIONS > 0 {
		r.Status = binary.LittleEndian.Uint16(data[pos:])
		self.status = r.Status
		pos += 2
	}

	//info
	return r, nil
}

func (self *Conn) handleErrorPacket(data []byte) error {
	e := new(mysql.MysqlError)

	var pos int = 1

	e.Code = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if self.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		//skip '#'
		pos++
		e.State = string(data[pos : pos+5])
		pos += 5
	}

	e.Message = string(data[pos:])

	return e
}

func (self *Conn) exec(query string) (*mysql.Result, error) {
	if err := self.writeCommandStr(mysql.COM_QUERY, query); err != nil {
		return nil, err
	}

	return self.readResult(false)
}

func (self *Conn) writeCommandStr(command byte, arg string) error {
	self.pkg.Sequence = 0

	length := len(arg) + 1

	data := make([]byte, length+4)

	data[4] = command

	copy(data[5:], arg)

	return self.pkg.WritePacket(data)
}

func (self *Conn) readResult(binary bool) (*mysql.Result, error) {
	data, err := self.pkg.ReadPacket()
	if err != nil {
		return nil, err
	}

	if data[0] == mysql.OK_HEADER {
		return self.handleOKPacket(data)
	} else if data[0] == mysql.ERR_HEADER {
		return nil, self.handleErrorPacket(data)
	} else if data[0] == mysql.LocalInFile_HEADER {
		return nil, mysql.ErrMalformPacket
	}

	return self.readResultset(data, binary)
}

func (self *Conn) readResultset(data []byte, binary bool) (*mysql.Result, error) {
	result := &mysql.Result{
		Status:       0,
		InsertId:     0,
		AffectedRows: 0,

		Resultset: &mysql.Resultset{},
	}

	// column count
	count, _, n := mysql.LengthEncodedInt(data)

	if n-len(data) != 0 {
		return nil, mysql.ErrMalformPacket
	}

	result.Fields = make([]*mysql.Field, count)
	result.FieldNames = make(map[string]int, count)

	if err := self.readResultColumns(result); err != nil {
		return nil, err
	}

	if err := self.readResultRows(result, binary); err != nil {
		return nil, err
	}

	return result, nil
}

func (self *Conn) readResultColumns(result *mysql.Result) (err error) {
	var i int = 0
	var data []byte

	for {
		data, err = self.pkg.ReadPacket()
		if err != nil {
			return
		}

		// EOF Packet
		if self.isEOFPacket(data) {
			if self.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
				//result.Warnings = binary.LittleEndian.Uint16(data[1:])
				//todo add strict_mode, warning will be treat as error
				result.Status = binary.LittleEndian.Uint16(data[3:])
				self.status = result.Status
			}

			if i != len(result.Fields) {
				err = mysql.ErrMalformPacket
			}

			return
		}

		result.Fields[i], err = mysql.FieldData(data).Parse()
		if err != nil {
			return
		}

		result.FieldNames[string(result.Fields[i].Name)] = i

		i++
	}
}

func (self *Conn) readResultRows(result *mysql.Result, isBinary bool) (err error) {
	var data []byte

	for {
		data, err = self.pkg.ReadPacket()

		if err != nil {
			return
		}

		// EOF Packet
		if self.isEOFPacket(data) {
			if self.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
				//result.Warnings = binary.LittleEndian.Uint16(data[1:])
				//todo add strict_mode, warning will be treat as error
				result.Status = binary.LittleEndian.Uint16(data[3:])
				self.status = result.Status
			}

			break
		}

		result.RowDatas = append(result.RowDatas, data)
	}

	result.Values = make([][]interface{}, len(result.RowDatas))

	for i := range result.Values {
		result.Values[i], err = result.RowDatas[i].Parse(result.Fields, isBinary)

		if err != nil {
			return err
		}
	}

	return nil
}

func (self *Conn) isEOFPacket(data []byte) bool {
	return data[0] == mysql.EOF_HEADER && len(data) <= 5
}

func (self *Conn) IsAutoCommit() bool {
	return self.status&mysql.SERVER_STATUS_AUTOCOMMIT > 0
}
