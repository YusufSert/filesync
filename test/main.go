package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	// Fast version (changes order)
	a := []string{"A", "B", "C", "D", "E"}
	i := 2
	noew := time.Now()
	// Remove the element at index i from a.
	a[i] = a[len(a)-1] // Copy last element to index i
	a[len(a)-1] = ""   // Erase last element (write zero value)
	a = a[:len(a)-1]   // Truncate slice.
	fmt.Println(time.Since(noew))
	fmt.Println(a)

	// Slow version
	b := []string{"A", "B", "C", "D", "E"}

	noew = time.Now()
	// Remove the element at index i from b
	copy(b[i:], b[i+1:]) // Shift b[i+1:] left one index.
	b[len(b)-1] = ""     // Erase last element (write zero value).
	b = b[:len(b)-1]     // Truncate slice

	f, err := os.OpenFile("./1/test.txt", os.O_RDONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	err = f.Close()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(f.Name())
	err = os.Rename("./1/test.txt", "./2/test.txt")
	if err != nil {
		log.Fatal(err)
	}

}
