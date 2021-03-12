package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"
)

var addr = flag.String("addr", "localhost:8080", "the http address of thing to be benchmarked")
var iters = flag.Int("iters", 1000, "number of iterations")
var routines = flag.Int("routines", 1, "how many concurrent goroutines wriring values at the same time")

func getFunctionTime(nm string, fn func()) {
	start := time.Now()
	var mx time.Duration
	min := time.Hour * 24

	for i := 0; i < *iters; i++ {
		iStart := time.Now()
		fn()
		iEnd := time.Since(iStart)

		if iEnd > mx {
			mx = iEnd
		}

		if iEnd < min {
			min = iEnd
		}
	}

	avg := time.Since(start) / time.Duration(*iters)
	qps := float64(*iters) / (float64(time.Since(start)) / float64(time.Second))
	fmt.Printf("Function: %s took %s on avg, %.1f QPS, %s max, %s min\n", nm, avg, qps, mx, min)
}

func writeRandomKey() {
	key := fmt.Sprintf("key-%d", rand.Intn(10000000))
	value := fmt.Sprintf("val-%d", rand.Intn(10000000))

	vals := url.Values{
		"key":   []string{key},
		"value": []string{value},
	}

	resp, err := http.Get("http://" + (*addr) + "/set?" + vals.Encode())
	if err != nil {
		log.Fatalf("error while sending request")
	}
	defer resp.Body.Close()
}

func main() {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()
	var wg sync.WaitGroup
	for i := 0; i < *routines; i++ {
		wg.Add(1)
		go func() {
			getFunctionTime("writeRandomKey", writeRandomKey)
			wg.Done()
		}()
	}

	wg.Wait()
}
