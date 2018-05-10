package vfilter

func ExtractString(name string, args *Dict) (*string, bool) {
	arg, ok := (*args).Get(name)
	if ok {
		arg_string, ok := arg.(string)
		if ok {
			return &arg_string, true
		}
	}

	return nil, false
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
