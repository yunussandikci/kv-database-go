package main

import "fmt"

func main() {
	database, databaseErr := NewKVDatabase[string]("myfile.json")
	if databaseErr != nil {
		panic(databaseErr)
	}

	database.Set("key", "value")
	if persisErr := database.Persist(); persisErr != nil {
		panic(persisErr)
	}

	fmt.Println(database.GetByKey("key"))
}
