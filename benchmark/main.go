package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
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
var readIterations = flag.Int("riters", 100000, "number of iterations for reading")

var client = &http.Client{
	Transport: &http.Transport{
		IdleConnTimeout:     time.Second * 60,
		MaxIdleConns:        300,
		MaxConnsPerHost:     300,
		MaxIdleConnsPerHost: 300,
	},
}

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

func commonBenchmark(fName string, iters int, fn func() string) (qps float64, strings []string) {
	var max time.Duration
	var min = time.Hour

	start := time.Now()
	for i := 0; i < iters; i++ {
		iterStart := time.Now()
		strings = append(strings, fn())
		iterTime := time.Since(iterStart)
		if iterTime > max {
			max = iterTime
		}
		if iterTime < min {
			min = iterTime
		}
	}

	avg := time.Since(start) / time.Duration(iters)
	qps = float64(iters) / (float64(time.Since(start)) / float64(time.Second))
	fmt.Printf("fn: %s, took: %s avg, qps: %1.f, max: %s, min: %s\n", fName, avg, qps, max, min)

	return qps, strings
}

func writeRand() (key string) {
	key = fmt.Sprintf("key-%d", rand.Intn(1000000))
	value := fmt.Sprintf("value-%d", rand.Intn(1000000))

	values := url.Values{}
	values.Set("key", key)
	values.Set("value", value)

	resp, err := client.Get("http://" + (*addr) + "/set?" + values.Encode())
	if err != nil {
		log.Fatalf("Error during set: %v", err)
	}

	io.Copy(ioutil.Discard, resp.Body)
	defer resp.Body.Close()

	return key
}

func benchmarkWrite() (keys []string) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var QPS float64

	for i := 0; i < *routines; i++ {
		wg.Add(1)
		go func() {
			qps, strings := commonBenchmark("write", *iters, writeRand)
			mu.Lock()
			QPS += qps
			keys = append(keys, strings...)
			mu.Unlock()

			wg.Done()
		}()
	}

	wg.Wait()
	log.Printf("qps: %.1f, keys len: %d", QPS, len(keys))

	return keys
}

func readRand(allKeys []string) (key string) {
	key = allKeys[rand.Intn(len(allKeys))]

	values := url.Values{}
	values.Set("key", key)

	resp, err := client.Get("http://" + (*addr) + "/get?" + values.Encode())
	if err != nil {
		log.Fatalf("Error during get: %v", err)
	}
	io.Copy(ioutil.Discard, resp.Body)
	defer resp.Body.Close()

	return key
}

func benchmarkRead(allKeys []string) {
	var totalQPS float64
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < *routines; i++ {
		wg.Add(1)
		go func() {
			qps, _ := commonBenchmark("read", *readIterations, func() string { return readRand(allKeys) })
			mu.Lock()
			totalQPS += qps
			mu.Unlock()

			wg.Done()
		}()
	}

	wg.Wait()

	log.Printf("Read total QPS: %.1f", totalQPS)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()

	keys := benchmarkWrite()
	go benchmarkWrite()

	benchmarkRead(keys)
}
