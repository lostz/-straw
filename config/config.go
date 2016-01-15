package config

type Config struct {
	Port         int
	Username     string
	Password     string
	MasterAddr   string
	Host         string
	ServerId     int
	MasterId     int
	GtidSlavePos string
}
