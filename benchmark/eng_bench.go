package main

import (
	"corekv/benchmark/storage_eng"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strconv"
	"sync"
	"time"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func RemoveDir(in string) {
	dir, _ := ioutil.ReadDir(in)
	for _, d := range dir {
		os.RemoveAll(path.Join([]string{in, d.Name()}...))
	}
}

func main() {

	if len(os.Args) < 2 {
		fmt.Println("bench_pmem [key_size] [value_size] [leveldb|pmem|corekv] [count] [thread count]")
		return
	}

	keySize, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic(err)
	}
	valSize, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	count, err := strconv.Atoi(os.Args[4])
	if err != nil {
		panic(err)
	}
	threadCount, err := strconv.Atoi(os.Args[5])
	if err != nil {
		panic(err)
	}

	keys := make([]string, count)
	vals := make([]string, count)

	startTs := time.Now()

	for i := 0; i < count; i++ {
		rndK := RandStringRunes(keySize)
		rndV := RandStringRunes(valSize)
		keys[i] = rndK
		vals[i] = rndV
	}

	switch os.Args[3] {
	case "pmem":
		fmt.Printf("start pmem bench\n")

		startTs = time.Now()

		wg := sync.WaitGroup{}
		for idx := 0; idx < threadCount; idx++ {
			pmemDBEng, err := storage_eng.MakePMemKvStore("127.0.0.1:6379")
			if err != nil {
				panic(err)
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < count; i++ {
					pmemDBEng.Put(keys[i], vals[i])
				}
			}()
		}
		wg.Wait()

		elapsed := time.Since(startTs).Seconds()
		fmt.Printf("total cost %f s\n", elapsed)
	case "leveldb":
		fmt.Printf("start leveldb bench\n")
		levelDBEng, err := storage_eng.MakeLevelDBKvStore("./leveldb_data")
		if err != nil {
			panic(err)
		}
		startTs = time.Now()
		wg := sync.WaitGroup{}

		for idx := 0; idx < threadCount; idx++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < count; i++ {
					levelDBEng.Put(keys[i], vals[i])
				}
			}()
		}
		wg.Wait()

		elapsed := time.Since(startTs).Seconds()
		fmt.Printf("total cost %f s\n", elapsed)
		RemoveDir("./leveldb_data")
	case "corekv":
		fmt.Printf("start albus bench\n")
		os.Mkdir("work_test", 0755)
		coreKVEng, err := storage_eng.MakeCoreKvStore("")
		if err != nil {
			panic(err)
		}
		startTs = time.Now()
		wg := sync.WaitGroup{}

		for idx := 0; idx < threadCount; idx++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < count; i++ {
					coreKVEng.Put(keys[i], vals[i])
				}
			}()
		}
		wg.Wait()

		elapsed := time.Since(startTs).Seconds()
		fmt.Printf("total cost %f s\n", elapsed)
		RemoveDir("./work_test")
	}
}
