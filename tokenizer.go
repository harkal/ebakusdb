package ebakusdb

import (
	"reflect"
)

// Tokenizer interface
type Tokenizer interface {
	Tokenize(content []byte) [][]byte
}

type tokenizer struct {
	seps map[uint8][][]byte
}

// New Tokenizer
func NewTokenizer(seps []string) Tokenizer {
	return &tokenizer{
		seps: convertSeparators(seps),
	}
}

func (t tokenizer) Tokenize(content []byte) [][]byte {
	length := len(content)
	cut := make([]int, length)

byteWalker:
	for i := 0; i < length; i++ {
		r := content[i]

		if len(t.seps[r]) > 0 {
			for _, sep := range t.seps[r] {
				sepLength := len(sep)
				if reflect.DeepEqual(content[i:i+sepLength], sep) {
					for y := 0; y < sepLength; y++ {
						cut[i+y] = 1
					}

					if i-1 > 0 && content[i-1] == []byte(" ")[0] {
						cut[i-1] = 2
					}

					i += sepLength - 1

					if length > i+1 && content[i+1] == []byte(" ")[0] {
						cut[i+1] = 2
					}

					break byteWalker
				}
			}
		}
	}

	tokens := [][]byte{}
	skipWhitespace := false
	currentCut := cut[0]
	lastCut := 0

	// can't start with separator
	if currentCut == 1 {
		return tokens
	}

	for i := 0; i < length; i++ {
		if cut[i] == 2 {
			skipWhitespace = true
			continue
		} else {
			if currentCut != cut[i] {
				to := i
				if skipWhitespace {
					skipWhitespace = false
					to--
				}
				tokens = append(tokens, content[lastCut:to])
				lastCut = i
			}

			currentCut = cut[i]
		}
	}

	if lastCut < length {
		tokens = append(tokens, content[lastCut:])
	}

	return tokens
}

func convertSeparators(seps []string) map[uint8][][]byte {
	separators := make(map[uint8][][]byte, len(seps))

	for _, r := range seps {
		b := []byte(r)
		separators[b[0]] = append(separators[b[0]], []byte(r))
	}

	return separators
}
