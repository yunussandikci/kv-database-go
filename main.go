package main

import "fmt"

func main() {
	database := NewKVDatabase[int, string]("myfile.json")

	database.Set(10, "value")
	database.Persist()

	value, exist := database.Get(10)
	fmt.Println("value:", value, "exist:", exist)
}
