package nt4

import (
	"fmt"
	"time"
)

// ClientOptions with defaults. See ClientOptions for serverAddress.
func DefaultClientOptions(serverAddress string) ClientOptions {
	return ClientOptions{
		ServerAddress:     serverAddress,
		Port:              DefaultPort,
		Identity:          "Go-NT4-Client",
		ReconnectInterval: time.Second,
		Logger:            NewDefaultLogger(LogLevelInfo),
	}
}

// FRC team number to a roboRIO server address.
// example: 2064 becomes "10.20.64.2".
func TeamNumberToAddress(teamNumber int) string {
	octet2 := teamNumber / 100
	octet3 := teamNumber % 100
	return fmt.Sprintf("10.%d.%d.2", octet2, octet3)
}

// NT4 type string to numeric ID.
func TypeStringToID(typeStr string) int {
	switch typeStr {
	case TypeBoolean:
		return DataTypeBoolean
	case TypeDouble:
		return DataTypeDouble
	case TypeInt:
		return DataTypeInt
	case TypeFloat:
		return DataTypeFloat
	case TypeString:
		return DataTypeString
	case TypeBooleanArray:
		return DataTypeBooleanArray
	case TypeDoubleArray:
		return DataTypeDoubleArray
	case TypeIntArray:
		return DataTypeIntArray
	case TypeFloatArray:
		return DataTypeFloatArray
	case TypeStringArray:
		return DataTypeStringArray
	default:
		return DataTypeBinary
	}
}

// NT4 type ID to string.
func TypeIDToString(typeID int) string {
	switch typeID {
	case DataTypeBoolean:
		return TypeBoolean
	case DataTypeDouble:
		return TypeDouble
	case DataTypeInt:
		return TypeInt
	case DataTypeFloat:
		return TypeFloat
	case DataTypeString:
		return TypeString
	case DataTypeBooleanArray:
		return TypeBooleanArray
	case DataTypeDoubleArray:
		return TypeDoubleArray
	case DataTypeIntArray:
		return TypeIntArray
	case DataTypeFloatArray:
		return TypeFloatArray
	case DataTypeStringArray:
		return TypeStringArray
	default:
		return TypeRaw
	}
}
