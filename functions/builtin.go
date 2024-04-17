package functions

import "www.velocidex.com/golang/vfilter/types"

func GetBuiltinFunctions() []types.FunctionInterface {
	return []types.FunctionInterface{
		_DictFunc{},
		_Timestamp{},
		_SplitFunction{},
		_IfFunction{},
		FormatFunction{},
		_GetFunction{},
		_EncodeFunction{},

		// Aggregate functions must not be implicitly copied. They are
		// copied deliberately using vfilter.CopyFunction()
		&_CountFunction{},
		&_SumFunction{},
		&_MinFunction{},
		&_MaxFunction{},
		&_EnumerateFunction{},
		FormatFunction{},
		LenFunction{},
	}
}
