package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"h12.me/sej"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("sej dump|offset [filename]")
		return
	}
	file, err := os.Open(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	switch os.Args[1] {
	case "dump":
		var msg sej.Message
		for {
			_, err := msg.ReadFrom(file)
			if err != nil {
				if err == io.EOF {
					fmt.Println("(EOF)")
					return
				}
				log.Fatal(err)
			}
			fmt.Println("offset:", msg.Offset)
			fmt.Printf("message: %x (%s)\n", msg.Value, string(msg.Value))
		}
	case "offset": // get latest offset
		jf, err := sej.ParseJournalFileName(".", os.Args[2])
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(jf.LatestOffset())
	}
}
