package protocols

func getRanges(field_name []*int64, array_length int64) (
	start_range, end_range int64) {
	if field_name[0] != nil {
		start_range = *field_name[0]
	}
	if start_range < 0 {
		start_range = array_length + start_range
	}
	if start_range < 0 {
		start_range = 0
	}

	if field_name[1] != nil {
		end_range = *field_name[1]
	} else {
		end_range = array_length
	}

	if end_range < 0 {
		end_range = array_length + end_range
	}
	if end_range < 0 {
		end_range = 0
	}

	return start_range, end_range
}
