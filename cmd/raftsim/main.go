// raftsim は決定論的 Raft シミュレーションの CLI。
//
//	raftsim run    -seed N            1 シード実行し結果を表示
//	raftsim soak   -seeds N [-from K] [-parallel P]   ランダムソーク
//	raftsim replay -seed N [-o FILE]  失敗シードを決定的に再現しトレース出力
//
// run/soak/replay はすべて sim.SoakOptions(seed) の同一構成を使うため、
// soak で見つかった失敗シードは replay で 100% 再現できる。
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"raftsim/sim"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "run":
		cmdRun(os.Args[2:])
	case "soak":
		cmdSoak(os.Args[2:])
	case "replay":
		cmdReplay(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: raftsim {run|soak|replay} [flags]")
	os.Exit(2)
}

func cmdRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	seed := fs.Int64("seed", 1, "シード")
	fs.Parse(args)

	ops, events, err := sim.RunSoakSeed(*seed)
	if err != nil {
		fmt.Printf("seed %d: VIOLATION: %v\n", *seed, err)
		os.Exit(1)
	}
	fmt.Printf("seed %d: OK (完了操作 %d, イベント %d)\n", *seed, ops, events)
}

func cmdSoak(args []string) {
	fs := flag.NewFlagSet("soak", flag.ExitOnError)
	seeds := fs.Int64("seeds", 10000, "実行するシード数")
	from := fs.Int64("from", 1, "開始シード")
	parallel := fs.Int("parallel", runtime.NumCPU(), "並列数")
	fs.Parse(args)

	start := time.Now()
	var (
		next     atomic.Int64
		done     atomic.Int64
		totalOps atomic.Int64
		totalEv  atomic.Int64
		mu       sync.Mutex
		failures []string
	)
	next.Store(*from)
	end := *from + *seeds

	var wg sync.WaitGroup
	for w := 0; w < *parallel; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				seed := next.Add(1) - 1
				if seed >= end {
					return
				}
				ops, events, err := sim.RunSoakSeed(seed)
				if err != nil {
					mu.Lock()
					failures = append(failures, fmt.Sprintf("seed %d: %v", seed, err))
					fmt.Printf("FAIL seed %d: %v\n", seed, err)
					mu.Unlock()
				}
				totalOps.Add(int64(ops))
				totalEv.Add(int64(events))
				if n := done.Add(1); n%1000 == 0 {
					fmt.Printf("... %d/%d シード完了 (%.0fs)\n", n, *seeds, time.Since(start).Seconds())
				}
			}
		}()
	}
	wg.Wait()

	el := time.Since(start)
	fmt.Printf("\nソーク完了: %d シード (3/5/7 ノード混合, 全障害有効), %.1fs\n", *seeds, el.Seconds())
	fmt.Printf("  完了クライアント操作: %d (%.0f ops/s 処理)\n", totalOps.Load(), float64(totalOps.Load())/el.Seconds())
	fmt.Printf("  処理イベント: %d (%.0f events/s)\n", totalEv.Load(), float64(totalEv.Load())/el.Seconds())
	if len(failures) > 0 {
		fmt.Printf("  違反: %d 件\n", len(failures))
		for _, f := range failures {
			fmt.Println("   ", f)
		}
		fmt.Println("再現: raftsim replay -seed <N>")
		os.Exit(1)
	}
	fmt.Println("  安全性不変条件・線形化可能性の違反: 0")
}

func cmdReplay(args []string) {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	seed := fs.Int64("seed", 1, "再現するシード")
	out := fs.String("o", "", "トレース出力先 (省略時 stdout)")
	fs.Parse(args)

	o := sim.SoakOptions(*seed)
	o.Trace = true
	s := sim.New(o)
	runErr := s.Run()
	linErr := error(nil)
	if runErr == nil {
		linErr = s.CheckLinearizable()
	}

	w := os.Stdout
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		defer f.Close()
		w = f
	}
	w.Write(s.Trace())

	switch {
	case runErr != nil:
		fmt.Fprintf(os.Stderr, "seed %d: VIOLATION (決定的に再現): %v\n", *seed, runErr)
		os.Exit(1)
	case linErr != nil:
		fmt.Fprintf(os.Stderr, "seed %d: 線形化違反 (決定的に再現): %v\n", *seed, linErr)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "seed %d: OK (イベント %d)\n", *seed, s.Events())
	}
}
