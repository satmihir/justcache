package internal

import "log"

func MustBeTrue(condition bool, msg string) {
	if !condition {
		log.Fatal(msg)
	}
}
