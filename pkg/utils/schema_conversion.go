package utils

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
)

// ArrowSchemaToIcebergSchema converts an Arrow schema to an Iceberg schema
func ArrowSchemaToIcebergSchema(arrowSchema *arrow.Schema) (*iceberg.Schema, error) {
	if arrowSchema == nil {
		return nil, fmt.Errorf("arrow schema is nil")
	}

	// Create Iceberg schema fields from Arrow fields
	var fields []iceberg.NestedField
	for i, field := range arrowSchema.Fields() {
		icebergType, err := arrowTypeToIcebergType(field.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s: %w", field.Name, err)
		}

		// Create a new Iceberg field
		fields = append(fields, iceberg.NestedField{
			ID:       i + 1, // Use 1-based IDs as per Iceberg convention
			Name:     field.Name,
			Type:     icebergType,
			Required: !field.Nullable,
		})
	}

	// Create a new Iceberg schema with the fields
	return iceberg.NewSchema(0, fields...), nil
}

// SchemaToString returns a string representation of an Arrow schema
func SchemaToString(schema *arrow.Schema) string {
	if schema == nil {
		return "nil schema"
	}

	result := "Schema:\n"
	for i, field := range schema.Fields() {
		result += fmt.Sprintf("  Field %d: %s (%s, nullable=%t)\n",
			i, field.Name, field.Type.String(), field.Nullable)
	}
	return result
}

// arrowFieldToIcebergField converts an Arrow field to an Iceberg nested field
func arrowFieldToIcebergField(id int, field arrow.Field) (iceberg.NestedField, error) {
	icebergType, err := arrowTypeToIcebergType(field.Type)
	if err != nil {
		return iceberg.NestedField{}, fmt.Errorf("failed to convert type: %w", err)
	}

	return iceberg.NestedField{
		ID:       id,
		Name:     field.Name,
		Type:     icebergType,
		Required: !field.Nullable,
	}, nil
}

// arrowTypeToIcebergType converts an Arrow data type to an Iceberg type
func arrowTypeToIcebergType(arrowType arrow.DataType) (iceberg.Type, error) {
	switch arrowType.ID() {
	case arrow.BOOL:
		return iceberg.BooleanType{}, nil
	case arrow.INT8, arrow.INT16, arrow.INT32:
		return iceberg.Int32Type{}, nil
	case arrow.INT64:
		return iceberg.Int64Type{}, nil
	case arrow.FLOAT32:
		return iceberg.Float32Type{}, nil
	case arrow.FLOAT64:
		return iceberg.Float64Type{}, nil
	case arrow.STRING:
		return iceberg.StringType{}, nil
	case arrow.BINARY:
		return iceberg.BinaryType{}, nil
	case arrow.FIXED_SIZE_BINARY:
		fsb := arrowType.(*arrow.FixedSizeBinaryType)
		return iceberg.FixedTypeOf(fsb.ByteWidth), nil
	case arrow.DATE32:
		return iceberg.DateType{}, nil
	case arrow.DATE64:
		return iceberg.DateType{}, nil
	case arrow.TIMESTAMP:
		ts := arrowType.(*arrow.TimestampType)
		if ts.TimeZone == "" {
			return iceberg.TimestampType{}, nil
		}
		return iceberg.TimestampTzType{}, nil
	case arrow.TIME32, arrow.TIME64:
		return iceberg.TimeType{}, nil
	case arrow.DECIMAL128:
		dt := arrowType.(*arrow.Decimal128Type)
		return iceberg.DecimalTypeOf(int(dt.Precision), int(dt.Scale)), nil
	case arrow.LIST:
		lt := arrowType.(*arrow.ListType)
		elemType, err := arrowTypeToIcebergType(lt.Elem())
		if err != nil {
			return nil, fmt.Errorf("failed to convert list element type: %w", err)
		}

		// Create list type with element field
		return &iceberg.ListType{
			ElementID:       1,
			Element:         elemType,
			ElementRequired: true,
		}, nil
	case arrow.STRUCT:
		st := arrowType.(*arrow.StructType)
		var fields []iceberg.NestedField
		for i, field := range st.Fields() {
			nestedField, err := arrowFieldToIcebergField(i+1, field)
			if err != nil {
				return nil, fmt.Errorf("failed to convert struct field: %w", err)
			}
			fields = append(fields, nestedField)
		}
		return &iceberg.StructType{FieldList: fields}, nil
	case arrow.MAP:
		mt := arrowType.(*arrow.MapType)
		keyType, err := arrowTypeToIcebergType(mt.KeyType())
		if err != nil {
			return nil, fmt.Errorf("failed to convert map key type: %w", err)
		}
		valueType, err := arrowTypeToIcebergType(mt.ItemType())
		if err != nil {
			return nil, fmt.Errorf("failed to convert map value type: %w", err)
		}

		// Create map type with key and value types
		return &iceberg.MapType{
			KeyID:         1,
			KeyType:       keyType,
			ValueID:       2,
			ValueType:     valueType,
			ValueRequired: true,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported arrow type: %s", arrowType.Name())
	}
}

// IcebergSchemaToArrowSchema converts an Iceberg schema to an Arrow schema
func IcebergSchemaToArrowSchema(icebergSchema *iceberg.Schema) (*arrow.Schema, error) {
	if icebergSchema == nil {
		return nil, fmt.Errorf("iceberg schema is nil")
	}

	var arrowFields []arrow.Field
	for i := 0; i < icebergSchema.NumFields(); i++ {
		field := icebergSchema.Field(i)
		arrowField, err := icebergFieldToArrowField(field)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s: %w", field.Name, err)
		}
		arrowFields = append(arrowFields, *arrowField)
	}

	return arrow.NewSchema(arrowFields, nil), nil
}

// icebergFieldToArrowField converts an Iceberg nested field to an Arrow field
func icebergFieldToArrowField(field iceberg.NestedField) (*arrow.Field, error) {
	arrowType, err := icebergTypeToArrowType(field.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to convert type: %w", err)
	}

	return &arrow.Field{
		Name:     field.Name,
		Type:     arrowType,
		Nullable: !field.Required,
	}, nil
}

// icebergTypeToArrowType converts an Iceberg type to an Arrow data type
func icebergTypeToArrowType(icebergType iceberg.Type) (arrow.DataType, error) {
	switch icebergType.String() {
	case "boolean":
		return arrow.FixedWidthTypes.Boolean, nil
	case "int":
		return arrow.PrimitiveTypes.Int32, nil
	case "long":
		return arrow.PrimitiveTypes.Int64, nil
	case "float":
		return arrow.PrimitiveTypes.Float32, nil
	case "double":
		return arrow.PrimitiveTypes.Float64, nil
	case "date":
		return arrow.FixedWidthTypes.Date32, nil
	case "time":
		return arrow.FixedWidthTypes.Time64us, nil
	case "timestamp":
		return arrow.FixedWidthTypes.Timestamp_us, nil
	case "timestamptz":
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
	case "string":
		return arrow.BinaryTypes.String, nil
	case "uuid":
		return &arrow.FixedSizeBinaryType{ByteWidth: 16}, nil
	case "binary":
		return arrow.BinaryTypes.Binary, nil
	default:
		// Handle complex types
		switch t := icebergType.(type) {
		case iceberg.DecimalType:
			precision := int32(t.Precision())
			scale := int32(t.Scale())
			return &arrow.Decimal128Type{Precision: precision, Scale: scale}, nil
		case *iceberg.DecimalType:
			precision := int32(t.Precision())
			scale := int32(t.Scale())
			return &arrow.Decimal128Type{Precision: precision, Scale: scale}, nil
		case iceberg.FixedType:
			return &arrow.FixedSizeBinaryType{ByteWidth: t.Len()}, nil
		case *iceberg.FixedType:
			return &arrow.FixedSizeBinaryType{ByteWidth: t.Len()}, nil
		case *iceberg.ListType:
			elemType, err := icebergTypeToArrowType(t.Element)
			if err != nil {
				return nil, fmt.Errorf("failed to convert list element type: %w", err)
			}
			return arrow.ListOf(elemType), nil
		case *iceberg.StructType:
			var fields []arrow.Field
			for _, field := range t.FieldList {
				arrowField, err := icebergFieldToArrowField(field)
				if err != nil {
					return nil, fmt.Errorf("failed to convert struct field: %w", err)
				}
				fields = append(fields, *arrowField)
			}
			return arrow.StructOf(fields...), nil
		case *iceberg.MapType:
			keyType, err := icebergTypeToArrowType(t.KeyType)
			if err != nil {
				return nil, fmt.Errorf("failed to convert map key type: %w", err)
			}
			valueType, err := icebergTypeToArrowType(t.ValueType)
			if err != nil {
				return nil, fmt.Errorf("failed to convert map value type: %w", err)
			}
			return arrow.MapOf(keyType, valueType), nil
		default:
			return nil, fmt.Errorf("unsupported iceberg type: %s", icebergType.String())
		}
	}
}
