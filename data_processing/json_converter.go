package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"strings"
	"sync"
)

type folder struct {
	Name       string   `json:"name"`
	Subfolders []folder `json:"subfolders"`
	Emails     []string `json:"emails"`
}

func main() {
	maildir := "../../enron_mail_20110402/maildir"
	files, err := os.ReadDir(maildir)
	if err != nil {
		log.Println(err)
	}

	var wg sync.WaitGroup

	for _, f := range files {
		wg.Add(1)
		go func(f os.DirEntry) {
			defer wg.Done()
			result := readFolder(maildir + "/" + f.Name())
			jsonData, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				log.Fatal(err)
			}
			err = os.WriteFile(f.Name()+".json", jsonData, 0644)
			if err != nil {
				log.Fatal(err)
			}
		}(f)
	}

	wg.Wait()

}

func readFolder(folder_name string) folder {
	var files, err = os.ReadDir(folder_name)
	var object folder
	object.Name = folder_name
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range files {
		if f.IsDir() {
			object.Subfolders = append(object.Subfolders, readFolder(folder_name+"/"+f.Name()))
		} else {
			object.Emails = append(object.Emails, processFile(folder_name+"/"+f.Name()))
		}
	}
	return object
}

func processFile(file string) string {
	const maxCapacity = 512 * 1024
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	data := make(map[string]interface{})
	buf := make([]byte, 0, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			data[key] = value
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	return string(jsonData)
}
