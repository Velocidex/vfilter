package protocols

import "www.velocidex.com/golang/vfilter/types"

func GetBuiltinTypes() []types.Any {
	return []types.Any{
		// Commented out protocols below are inlined for performance.

		// Most common objects come first to optimise O(n) algorithm.
		// _ScopeAssociative{},	_Lazytypes.RowAssociative{}, _DictAssociative{}, _types.NullAssociative{},
		_StoredQueryAssociative{},

		// _types.NullBoolProtocol{}, _BoolImpl{}, _BoolInt{}, _BoolString{},
		// _BoolSlice{}, _BoolDict{},
		_StoredQueryBool{},

		// _types.NullEqProtocol{}, _StringEq{}, _IntEq{}, _NumericEq{},
		// _ArrayEq{},
		_DictEq{},

		// _NumericLt{}, _StringLt{},

		// _AddStrings{}, _AddInts{}, _AddFloats{}, _AddSlices{}, _AddSliceAny{}, _AddNull{},
		_StoredQueryAdd{},

		// _SubInts{}, _SubFloats{},
		//_SubstringMembership{},

		// _MulInt{}, _NumericMul{},
		// _NumericDiv{},

		// _SubstringRegex{},

		// _ArrayRegex{},

		// _SliceIterator{}, // _LazyExprIterator{}, _StoredQueryIterator{}, _DictIterator{},
	}
}
