package utils

import (
	"errors"
	"strconv"
	"strings"
)

// unquote either a " or ' delimited string.
func Unquote(s string) string {
	// If we start with ''' this is a literal string - do not
	// interfer at all.
	if strings.HasPrefix(s, "'''") {
		if strings.HasSuffix(s, "'''") {
			return s[3 : len(s)-3]
		}
	}

	// This should not happen but in case it does do not interfer at all.
	quote := s[0]
	if quote != '"' && quote != '\'' {
		return s
	}

	// Now decode the string. Go's strconv.Unquote converts
	// characters to utf8 which will corrupt hex escapes (e.g \xf4
	// will be encoded as utf8). VQL does not treat strings
	// especially at all (i.e. they are just a sequence of bytes)
	// and does not allow utf8 escapes anyway. If the original
	// string contains utf8 then the unquoted string will contain
	// the same sequence. So we do not want to parse the code
	// points here all - just pass them along but convert any
	// escapes as needed.
	in := s[1 : len(s)-1]
	out := make([]byte, len(in))
	end := len(in) - 1
	i := 0
	j := 0

outer:
	for {
		if i > end {
			break outer
		}

		switch in[i] {
		case '\\':
			// Invalid escape at the end of string.
			if i >= end {
				break outer
			}

			switch in[i+1] {
			// Hex encoded byte
			case 'x', 'X':
				// Invalid escape at end of string.
				if i > end-3 {
					break outer
				}

				decoded, err := decode_hex(in[i+2], in[i+3])
				// Invalid escape sequence we just
				// ignore it and copy the output
				// verbatim.
				if err != nil {
					out[j] = in[j]
					i++
					j++
				} else {
					out[j] = decoded
					i += 4
					j++
				}

			case 'r':
				out[j] = '\r'
				i += 2
				j++

			case 'n':
				out[j] = '\n'
				i += 2
				j++

			case 't':
				out[j] = '\t'
				i += 2
				j++

			case '\\', '"', '\'':
				out[j] = in[i+1]
				i += 2
				j++

			default:
				out[j] = in[i+1]
				i += 2
				j++
			}
		default:
			out[j] = in[i]
			i++
			j++
		}
	}
	return string(out[:j])
}

// Unquote a ` delimited string.
func Unquote_ident(s string) string {
	quote := s[0]
	if quote != '`' {
		return s
	}

	s = s[1 : len(s)-1]
	out := ""
	for s != "" {
		value, _, tail, err := strconv.UnquoteChar(s, quote)
		if err != nil {
			return s
		}
		s = tail
		out += string(value)
	}
	return out
}

func decode_hex(left, right uint8) (uint8, error) {
	res := hex_lookup[left]
	if res < 0 {
		return 0, invalidError
	}

	res2 := hex_lookup[right]
	if res2 < 0 {
		return 0, invalidError
	}

	return uint8(res<<4 | res2), nil
}

var (
	hex_lookup   = [256]int8{}
	invalidError = errors.New("Invalid")
)

func init() {
	for i := 0; i < 256; i++ {
		hex_lookup[i] = -1
	}

	for i := int8('0'); i <= int8('9'); i++ {
		hex_lookup[i] = i - int8('0')
	}

	for i := int8('a'); i <= int8('f'); i++ {
		hex_lookup[i] = i - int8('a') + 10
	}

	for i := int8('A'); i <= int8('F'); i++ {
		hex_lookup[i] = i - int8('A') + 10
	}
}
