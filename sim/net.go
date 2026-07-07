package sim

import "math/rand"

// NetParams はネットワーク障害モデルのパラメータ (SPEC §3.3)。
type NetParams struct {
	DropProb  float64 // メッセージ喪失確率 (0〜0.30)
	DupProb   float64 // メッセージ重複確率 (0〜0.10)
	DelayMin  int64   // 最小遅延 (µs)
	DelayMean int64   // 指数分布の平均遅延 (µs)
	DelayMax  int64   // 最大遅延 (µs)。順序入替は遅延分布により自然発生する
}

// Network はメッセージ配送を仮想時間イベントとしてスケジュールする。
// 分断はノード→グループIDの割当で表現し、異なるグループ間の送信を
// 送信時点で落とす (DECISIONS.md D-013)。
type Network struct {
	p     NetParams
	rng   *rand.Rand
	q     *Queue
	trace *Trace
	// partition[node] = グループID。未登録ノードはグループ0。
	partition map[uint64]int
}

func NewNetwork(p NetParams, rng *rand.Rand, q *Queue, trace *Trace) *Network {
	return &Network{p: p, rng: rng, q: q, trace: trace, partition: map[uint64]int{}}
}

// SetPartition はノードのグループ割当を差し替える。nil または空で全接続に戻る。
func (n *Network) SetPartition(groups map[uint64]int) {
	n.partition = map[uint64]int{}
	for k, v := range groups { // 参照のみの複製。順序は挙動に影響しない
		n.partition[k] = v
	}
}

func (n *Network) group(id uint64) int { return n.partition[id] }

// Partitioned は from→to が分断で遮断されているかを返す。
func (n *Network) Partitioned(from, to uint64) bool {
	return n.group(from) != n.group(to)
}

func (n *Network) delay() int64 {
	mean := float64(n.p.DelayMean - n.p.DelayMin)
	if mean < 1 {
		mean = 1
	}
	d := n.p.DelayMin + int64(n.rng.ExpFloat64()*mean)
	if n.p.DelayMax > 0 && d > n.p.DelayMax {
		d = n.p.DelayMax
	}
	return d
}

// Send は from→to のメッセージ配送をスケジュールする。deliver は配送時刻に
// 呼ばれるクロージャ。ignorePartition はクライアント通信用 (D-007)。
// 喪失・重複・遅延の乱数消費順序は固定(drop→delay→dup)であり決定論を保つ。
func (n *Network) Send(now int64, from, to uint64, ignorePartition bool, desc string, deliver func()) {
	if !ignorePartition && n.Partitioned(from, to) {
		n.trace.Logf(now, "net: partition-drop %d->%d %s", from, to, desc)
		return
	}
	if n.p.DropProb > 0 && n.rng.Float64() < n.p.DropProb {
		n.trace.Logf(now, "net: drop %d->%d %s", from, to, desc)
		return
	}
	d := n.delay()
	n.q.At(now+d, deliver)
	if n.p.DupProb > 0 && n.rng.Float64() < n.p.DupProb {
		d2 := n.delay()
		n.trace.Logf(now, "net: dup %d->%d %s", from, to, desc)
		n.q.At(now+d2, deliver)
	}
}
