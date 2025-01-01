package datatype

import "errors"

func (dts *DataTypeService) Del(key []byte) error {
	return dts.db.Delete(key)
}

func (dts *DataTypeService) Type(key []byte) (DataType, error) {
	encValue, err := dts.db.Get(key)
	if err != nil {
		return 0, err
	}

	if len(encValue) == 0 {
		return 0, errors.New("value is null")
	}

	return encValue[0], nil
}
