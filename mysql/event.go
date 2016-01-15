package mysql

type GTIDEvent struct {
	CommitFlag uint8
	DID        uint32
	SID        uint32
	SNum       uint64
}
