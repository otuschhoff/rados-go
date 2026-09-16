package protocol

const (
	MessageMonMap            uint16 = 4
	MessageStatFS            uint16 = 13
	MessageStatFSReply       uint16 = 14
	MessageMonSubscribe      uint16 = 15
	MessageMonSubscribeAck   uint16 = 16
	MessageOSDOp             uint16 = 42
	MessageOSDOpReply        uint16 = 43
	MessageWatchNotify       uint16 = 44
	MessagePoolOpReply       uint16 = 48
	MessagePoolOp            uint16 = 49
	MessageGetPoolStats      uint16 = 58
	MessageGetPoolStatsReply uint16 = 59
	MessageOSDBackoff        uint16 = 61
	MessageOSDMap            uint16 = 41
	MessageMonCommand        uint16 = 50
	MessageMonCommandAck     uint16 = 51
	MessageCommand           uint16 = 97
	MessageCommandReply      uint16 = 98
	MessageMgrMap            uint16 = 0x704
	MessageMgrCommand        uint16 = 0x709
	MessageMgrCommandReply   uint16 = 0x70a
)
