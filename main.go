package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type (
	ammount      float64
	movementType string
	user         string

	movement struct {
		User    user
		Type    movementType
		Ammount ammount
	}

	result struct {
		Count map[movementType]float64
		Sum   map[movementType]ammount
		Avg   map[movementType]float64
	}
)

func newMovement(ms string) (*movement, error) {
	var m movement

	mf := strings.Fields(ms)
	for _, f := range mf {
		idx := strings.Index(f, ":")
		if idx > -1 {
			fv := f[idx+1 : len(f)-1]

			switch f[:idx] {
			case "[user":
				m.User = user(fv)
			case "[type":
				m.Type = movementType(fv)
			case "[ammount":
				if s, err := strconv.ParseUint(fv, 10, 64); err == nil {
					m.Ammount = ammount(s)
				} else {
					return nil, errors.New("Unable to parse ammount: " + fv)
				}
			}
		} else {
			return nil, errors.New("Record is not a movement")
		}
	}

	return &m, nil
}

func newResult() *result {
	var result result

	result.Sum = make(map[movementType]ammount)
	result.Count = make(map[movementType]float64)
	result.Avg = make(map[movementType]float64)

	return &result
}

func movementWorker(id int, movements chan movement, results chan result) {
	log.Printf("Starting worker: %d\n", id)

	result := newResult()

	for {
		m, ok := <-movements
		if !ok {
			log.Printf("Finishing worker: %d\n", id)

			results <- *result
			return
		}

		result.Sum[m.Type] += m.Ammount
		result.Count[m.Type]++

		// log.Printf("Processing worker: %d, type: %s, count: %f\n", id, m.Type, result.Count[m.Type])
		// time.Sleep(time.Millisecond * 1500)
	}
}

func processAsync(file *os.File) *result {
	wg := new(sync.WaitGroup)

	var movements = make(chan movement)
	var results = make(chan result)

	for wid := 1; wid <= 3; wid++ {
		wg.Add(1)
		go func(id int) {
			movementWorker(id, movements, results)
			wg.Done()
		}(wid)
	}

	go func() {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			m, err := newMovement(scanner.Text())
			if err == nil {
				movements <- *m
			}
		}
		close(movements)
	}()

	go func() {
		log.Println("Waiting for workers")
		wg.Wait()
		close(results)
	}()

	result := newResult()
	for wResult := range results {
		for k, count := range wResult.Count {
			result.Count[k] += count
			result.Sum[k] += wResult.Sum[k]
		}
	}

	for k, s := range result.Sum {
		result.Avg[k] = float64(s) / result.Count[k]
	}

	return result
}

func elapsed(what string) func() {
	start := time.Now()
	return func() {
		fmt.Printf("%s took %v\n", what, time.Since(start))
	}
}

func main() {
	defer elapsed("Movements process")()

	file, err := os.Open("files/movements-l.log")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	r := processAsync(file)

	log.Println(r)

	log.Println("Done!")
}
