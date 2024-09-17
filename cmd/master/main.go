package main

import (
	"fmt"
	"os"
	"time"

	mr "github.com/paulniziolek/mapreduce/pkg/mapreduce"
)

const (
	nReduce = 10
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeMaster(os.Args[1:], nReduce)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
