package datatype

import "errors"

// Del 删除key
func (dts *DataTypeService) Del(key []byte) error {
	return dts.db.Delete(key)
}

// Type 获取key的数据类型
func (dts *DataTypeService) Type(key []byte) (dataType, error) {
	encValue, err := dts.db.Get(key)
	if err != nil {
		return 0, err
	}

	if len(encValue) == 0 {
		return 0, errors.New("value is null")
	}

	return encValue[0], nil
}
