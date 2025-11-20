package nt4

// NT4 type strings.
const (
	TypeBoolean      = "boolean"
	TypeDouble       = "double"
	TypeInt          = "int"
	TypeFloat        = "float"
	TypeString       = "string"
	TypeBooleanArray = "boolean[]"
	TypeDoubleArray  = "double[]"
	TypeIntArray     = "int[]"
	TypeFloatArray   = "float[]"
	TypeStringArray  = "string[]"
	TypeRaw          = "raw"
	TypeMsgpack      = "msgpack"
	TypeProtobuf     = "protobuf"
	TypeJSON         = "json"
)

// NT4 type binaries.
const (
	DataTypeBoolean      = 0
	DataTypeDouble       = 1
	DataTypeInt          = 2
	DataTypeFloat        = 3
	DataTypeString       = 4
	DataTypeBinary       = 5
	DataTypeBooleanArray = 16
	DataTypeDoubleArray  = 17
	DataTypeIntArray     = 18
	DataTypeFloatArray   = 19
	DataTypeStringArray  = 20
)

// Default port for NT4 is on 5810.
const DefaultPort = 5810
