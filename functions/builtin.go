package functions

import "www.velocidex.com/golang/vfilter/types"

func GetBuiltinFunctions() []types.FunctionInterface {
	return []types.FunctionInterface{
		_DictFunc{},
		_Timestamp{},
		_SplitFunction{},
		_IfFunction{},
		_GetFunction{},
		_EncodeFunction{},
		_CountFunction{},
		_MinFunction{},
		_MaxFunction{},
		_EnumerateFunction{},
		LenFunction{},
	}
}
