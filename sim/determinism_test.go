package sim

import (
	"bytes"
	"math/rand"
	"testing"
)

// M0 受け入れテスト: 同一シードの2回実行でイベントトレースがバイト一致すること。
// raft 実装前のため、キュー・ネットワーク・トレース・PRNG を実際に使う
// トイプロセス(ランダムな相手にランダムペイロードを送り合う)で検証する。
// このテストは以降のマイルストーンでも sim 基盤の決定論の回帰テストとして残す。

func runToyWorld(seed int64) []byte {
	rng := rand.New(rand.NewSource(seed))
	q := &Queue{}
	tr := NewTrace(true)
	net := NewNetwork(NetParams{
		DropProb:  0.20,
		DupProb:   0.10,
		DelayMin:  1 * Millisecond,
		DelayMean: 5 * Millisecond,
		DelayMax:  50 * Millisecond,
	}, rng, q, tr)

	const nprocs = 5
	const horizon = 500 * Millisecond
	received := make([]int, nprocs+1)

	var now int64

	deliver := func(to uint64, payload int) func() {
		return func() {
			received[to]++
			tr.Logf(now, "proc %d: recv payload=%d total=%d", to, payload, received[to])
		}
	}

	var scheduleTick func(id uint64, interval int64)
	scheduleTick = func(id uint64, interval int64) {
		q.At(now+interval, func() {
			// ランダムな相手へランダムペイロードを送る
			to := uint64(rng.Intn(nprocs) + 1)
			payload := rng.Intn(1 << 20)
			tr.Logf(now, "proc %d: send to=%d payload=%d", id, to, payload)
			net.Send(now, id, to, false, "toy", deliver(to, payload))
			scheduleTick(id, interval)
		})
	}

	// クロックスキュー: ノードごとに tick 間隔へ ±20% の倍率
	for id := uint64(1); id <= nprocs; id++ {
		skew := 0.8 + 0.4*rng.Float64()
		interval := int64(float64(10*Millisecond) * skew)
		scheduleTick(id, interval)
	}

	// 途中で分断を入れ、後に回復させる(分断コードパスも決定論検査に含める)
	q.At(150*Millisecond, func() {
		tr.Logf(now, "fault: partition {1,2} | {3,4,5}")
		net.SetPartition(map[uint64]int{1: 0, 2: 0, 3: 1, 4: 1, 5: 1})
	})
	q.At(300*Millisecond, func() {
		tr.Logf(now, "fault: heal partition")
		net.SetPartition(nil)
	})

	for {
		t, fn, ok := q.Pop()
		if !ok || t > horizon {
			break
		}
		now = t
		fn()
	}
	return tr.Bytes()
}

func TestDeterminismSameSeed(t *testing.T) {
	for _, seed := range []int64{1, 42, 12345} {
		a := runToyWorld(seed)
		b := runToyWorld(seed)
		if len(a) == 0 {
			t.Fatalf("seed %d: トレースが空", seed)
		}
		if !bytes.Equal(a, b) {
			t.Fatalf("seed %d: 2回実行のトレースが一致しない (len %d vs %d)", seed, len(a), len(b))
		}
	}
}

func TestDifferentSeedsDiverge(t *testing.T) {
	a := runToyWorld(1)
	b := runToyWorld(2)
	if bytes.Equal(a, b) {
		t.Fatal("異なるシードで同一トレース: PRNG がシードに依存していない疑い")
	}
}

func TestQueueTieBreakBySeq(t *testing.T) {
	q := &Queue{}
	var got []int
	// 同時刻イベントは挿入順に実行されること
	for i := 0; i < 100; i++ {
		i := i
		q.At(10, func() { got = append(got, i) })
	}
	for {
		_, fn, ok := q.Pop()
		if !ok {
			break
		}
		fn()
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("同時刻イベントの実行順が挿入順でない: got[%d]=%d", i, v)
		}
	}
}
