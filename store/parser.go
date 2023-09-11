package store

import (
	"fmt"
	"time"
)

type Row map[string]interface{}

func (r Row) AsBytes(colName string) []byte {
	if o := r[colName]; o != nil {
		return o.([]byte)
	}
	return nil
}

func (r Row) getStringAsBytes(colName string) []byte {
	s := r.AsString(colName)
	if s == "" {
		return nil
	}
	return []byte(s)
}

func (r Row) AsString(colName string) string {
	if o := r[colName]; o != nil {
		return o.(string)
	}
	return ""
}

func (r Row) AsTimeDate(colName string) time.Time {
	if o := r[colName]; o != nil {
		switch t := o.(type) {
		case string:
			tm, _ := time.Parse("2006-01-02 15:04:05", t)
			return tm
		case int64:
			time.UnixMilli(t)
		default:
			panic(fmt.Sprintf("unknown type %T on column %s=%+v", o, colName, o))
		}
	}
	return time.Time{}
}

func (r Row) AsUint32(colName string) uint32 {
	if o := r[colName]; o != nil {
		return uint32(o.(int32))
	}
	return 0
}

func (r Row) AsBool(colName string) bool {
	if o := r[colName]; o != nil {
		return o.(int8) == 1
	}
	return false
}
