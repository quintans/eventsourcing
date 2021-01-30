package mongodb

import (
	"log"
	"os"
	"testing"
)

func TestMain(m *testing.M) {

	tearDown, err := Setup("./docker-compose.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// test run
	var code int
	func() {
		defer tearDown()
		code = m.Run()
	}()

	os.Exit(code)
}
