package services

import "sync"

func IndexData(jsonData []byte, wg *sync.WaitGroup) {
	defer wg.Done()
}
