package vfilter

func ExtractString(name string, args *Dict) (*string, bool) {
	if arg, pres := args.Get(name); pres {
		if arg_string, ok := arg.(string); ok {
			return &arg_string, true
		}
	}

	return nil, false
}

func ExtractFloat(output *float64, name string, args *Dict) bool {
	if arg, pres := args.Get(name); pres {
		if arg_float, ok := to_float(arg); ok {
			*output = arg_float
			return true
		}
	}

	return false
}

func ExtractStringArray(name string, args *Dict) ([]string, bool) {
	arg, ok := (*args).Get(name)
	if ok {
		arg_string, ok := arg.([]string)
		if ok {
			return arg_string, true
		}
	}

	return nil, false
}
