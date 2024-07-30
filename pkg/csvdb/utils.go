package csvdb

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/pkg/errors"
)

// https://github.com/golang/go/blob/master/src/database/sql/convert.go
func asString(src interface{}) string {
	switch v := src.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}
	rv := reflect.ValueOf(src)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 64)
	case reflect.Float32:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 32)
	case reflect.Bool:
		srci := 1
		if src == false {
			srci = 0
		}
		rv = reflect.ValueOf(srci)
		return strconv.FormatInt(rv.Int(), 10)
	}
	return fmt.Sprintf("%v", src)
}

func convFromString(src string, dest interface{}) error {
	sv := reflect.ValueOf(src)
	dpv := reflect.ValueOf(dest)
	errNilPtr := errors.New("destination pointer is nil")

	if dpv.Kind() != reflect.Ptr {
		return errors.New("destination not a pointer")
	}
	if dpv.IsNil() {
		return errNilPtr
	}

	dv := reflect.Indirect(dpv)

	if dv.Kind() == sv.Kind() && sv.Type().ConvertibleTo(dv.Type()) {
		dv.Set(sv.Convert(dv.Type()))
		return nil
	}

	switch dv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i64, err := strconv.ParseInt(src, 10, dv.Type().Bits())
		if err != nil {
			return err
		}
		dv.SetInt(i64)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u64, err := strconv.ParseUint(src, 10, dv.Type().Bits())
		if err != nil {
			return err
		}
		dv.SetUint(u64)

	case reflect.Float32, reflect.Float64:
		f64, err := strconv.ParseFloat(src, dv.Type().Bits())
		if err != nil {
			return err
		}
		dv.SetFloat(f64)

	case reflect.String:
		dv.SetString(src)

	case reflect.Bool:
		b, err := strconv.ParseBool(src)
		if err != nil {
			return err
		}
		dv.SetBool(b)
	}
	return nil
}

func ScanRow(row []string, args ...interface{}) error {
	if len(row) != len(args) {
		return errors.New(fmt.Sprintf("Got %d args while expected %d",
			len(args), len(row)))
	}
	for i, v := range row {
		if err := convFromString(v, args[i]); err != nil {
			return err
		}
	}
	return nil
}
