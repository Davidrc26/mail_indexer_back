package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"
)

type data struct {
	Index   string  `json:"index"`
	Records []email `json:"records"`
}

type email struct {
	ID                        int    `json:"ID"`
	Message_ID                string `json:"Message-ID"`
	Date                      string `json:"Date"`
	From                      string `json:"from"`
	To                        string `json:"to"`
	Subject                   string `json:"subject"`
	Mime_Version              string `json:"Mime-Version"`
	Content_Type              string `json:"Content-Type"`
	Content_Transfer_Encoding string `json:"Content-Transfer-Encoding"`
	X_From                    string `json:"X-From"`
	X_To                      string `json:"X-To"`
	X_cc                      string `json:"X-cc"`
	X_bcc                     string `json:"X-bcc"`
	X_Folder                  string `json:"X-Folder"`
	X_Origin                  string `json:"X-Origin"`
	X_FileName                string `json:"X-FileName"`
	Cc                        string `json:"Cc"`
	Body                      string `json:"Body"`
}

func main() {
	cpu, err := os.Create("profiling/cpu.prof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(cpu)
	defer pprof.StopCPUProfile()

	logFile, err := os.OpenFile("logsindexer/log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()

	log.SetOutput(logFile)
	start := time.Now()
	startIndexing()
	end := time.Now()

	fmt.Println("Time taken: ", end.Sub(start))
	log.Println("Time taken: ", end.Sub(start))
	log.Println("Execution finished")

	runtime.GC()
	mem, err := os.Create("profiling/memory.prof")
	if err != nil {
		log.Fatal(err)
	}
	defer mem.Close()
	if err := pprof.WriteHeapProfile(mem); err != nil {
		log.Fatal(err)
	}

}

func startIndexing() {
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
			bulk := data{Index: "maildir", Records: result}
			jsonData, err := json.MarshalIndent(bulk, "", "  ")
			if err != nil {
				log.Println(err)
			}
			IndexData(jsonData, f.Name())
		}(f)
	}
	wg.Wait()
}

func readFolder(folder_name string) []email {
	var files, err = os.ReadDir(folder_name)
	var object = make([]email, 0)
	if err != nil {
		log.Println("Error leyendo el directorio" + folder_name + "\nDetalles: " + err.Error())
		return object
	}
	for _, f := range files {
		if f.IsDir() {
			object = append(object, readFolder(folder_name+"/"+f.Name())...)
		} else {
			object = append(object, processFile(folder_name+"/"+f.Name()))
		}
	}
	return object
}

func processFile(file string) email {
	const maxCapacity = 512 * 1024
	f, err := os.Open(file)

	if err != nil {
		log.Println("Error procesando el archivo " + file + "\nDetalles: " + err.Error())
		return email{}
	}

	defer f.Close()
	scanner := bufio.NewScanner(f)
	data := email{}
	buf := make([]byte, 0, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {
		line := scanner.Text()
		i := strings.Index(line, ":")
		if i >= 0 {
			key := strings.TrimSpace(line[:i])
			value := strings.TrimSpace(line[i+1:])
			switch key {
			case "Message-ID":
				data.Message_ID = value
			case "Date":
				data.Date = value
			case "From":
				data.From = value
			case "To":
				data.To = value
			case "Subject":
				data.Subject = value
			case "Mime-Version":
				data.Mime_Version = value
			case "Content-Type":
				data.Content_Type = value
			case "Content-Transfer-Encoding":
				data.Content_Transfer_Encoding = value
			case "X-From":
				data.X_From = value
			case "X-To":
				data.X_To = value
			case "X-cc":
				data.X_cc = value
			case "X-bcc":
				data.X_bcc = value
			case "X-Folder":
				data.X_Folder = value
			case "X-Origin":
				data.X_Origin = value
			case "X-FileName":
				data.X_FileName = value
			case "Cc":
				data.Cc = value
			default:
				data.Body += value
			}
		}
	}
	return data
}

func IndexData(jsonData []byte, index string) {
	req, err := http.NewRequest("POST", "http://localhost:4080/api/_bulkv2", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Println("Error generando la request para la indexación de la carpeta de: " + index + "\nDetalles:" + err.Error())
		return
	}
	req.SetBasicAuth("yourZincsearchUser", "yourZincsearchPassword")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println("Error realizando la solicitud para crear e indexar el documento en el servidor de zincsearch\n" + "Directorio: " + index + "\nDetalles: " + err.Error())
		return
	}
	defer resp.Body.Close()
	log.Println(resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("Error leyendo el body de la respuesta del servidor de zincsearch\n" + "Detalles: " + err.Error())
		return
	}
	fmt.Println(string(body))
}
