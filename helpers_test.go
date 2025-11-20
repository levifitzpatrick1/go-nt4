package nt4

import "testing"

func TestTeamNumberToAddress(t *testing.T) {
	tests := []struct {
		name       string
		teamNumber int
		expected   string
	}{
		{"Team 2064", 2064, "10.20.64.2"},
		{"Team 254", 254, "10.2.54.2"},
		{"Team 1", 1, "10.0.1.2"},
		{"Team 9999", 9999, "10.99.99.2"},
		{"Team 100", 100, "10.1.0.2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TeamNumberToAddress(tt.teamNumber)
			if result != tt.expected {
				t.Errorf("TeamNumberToAddress(%d) = %s; want %s", tt.teamNumber, result, tt.expected)
			}
		})
	}
}

func TestTypeStringToID(t *testing.T) {
	tests := []struct {
		typeStr  string
		expected int
	}{
		{TypeBoolean, DataTypeBoolean},
		{TypeDouble, DataTypeDouble},
		{TypeInt, DataTypeInt},
		{TypeFloat, DataTypeFloat},
		{TypeString, DataTypeString},
		{TypeBooleanArray, DataTypeBooleanArray},
		{TypeDoubleArray, DataTypeDoubleArray},
		{TypeIntArray, DataTypeIntArray},
		{TypeFloatArray, DataTypeFloatArray},
		{TypeStringArray, DataTypeStringArray},
		{TypeRaw, DataTypeBinary},
		{TypeMsgpack, DataTypeBinary},
		{TypeProtobuf, DataTypeBinary},
		{TypeJSON, DataTypeBinary},
		{"unknown", DataTypeBinary},
	}

	for _, tt := range tests {
		t.Run(tt.typeStr, func(t *testing.T) {
			result := TypeStringToID(tt.typeStr)
			if result != tt.expected {
				t.Errorf("TypeStringToID(%s) = %d; want %d", tt.typeStr, result, tt.expected)
			}
		})
	}
}

func TestTypeIDToString(t *testing.T) {
	tests := []struct {
		typeID   int
		expected string
	}{
		{DataTypeBoolean, TypeBoolean},
		{DataTypeDouble, TypeDouble},
		{DataTypeInt, TypeInt},
		{DataTypeFloat, TypeFloat},
		{DataTypeString, TypeString},
		{DataTypeBooleanArray, TypeBooleanArray},
		{DataTypeDoubleArray, TypeDoubleArray},
		{DataTypeIntArray, TypeIntArray},
		{DataTypeFloatArray, TypeFloatArray},
		{DataTypeStringArray, TypeStringArray},
		{DataTypeBinary, TypeRaw},
		{999, TypeRaw},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := TypeIDToString(tt.typeID)
			if result != tt.expected {
				t.Errorf("TypeIDToString(%d) = %s; want %s", tt.typeID, result, tt.expected)
			}
		})
	}
}

func TestTypeConversionRoundTrip(t *testing.T) {
	types := []string{
		TypeBoolean,
		TypeDouble,
		TypeInt,
		TypeFloat,
		TypeString,
		TypeBooleanArray,
		TypeDoubleArray,
		TypeIntArray,
		TypeFloatArray,
		TypeStringArray,
	}

	for _, typeStr := range types {
		t.Run(typeStr, func(t *testing.T) {
			id := TypeStringToID(typeStr)
			result := TypeIDToString(id)
			if result != typeStr {
				t.Errorf("Round trip failed for %s: got %s", typeStr, result)
			}
		})
	}
}
