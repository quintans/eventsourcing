package test

import (
	"fmt"
	"math/rand"
)

func RandStr(str string) string {
	return fmt.Sprintf("%s%d", str, rand.Intn(10000))
}
