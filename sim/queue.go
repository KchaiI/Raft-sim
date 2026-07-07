package sim

import "container/heap"

// 仮想時間の単位はマイクロ秒 (DECISIONS.md D-001)。
const (
	Microsecond int64 = 1
	Millisecond int64 = 1000 * Microsecond
	Second      int64 = 1000 * Millisecond
)

// event はイベントキューの1要素。同時刻のイベントは挿入順序 seq で
// 決定論的に順序付ける (NOTES.md: heap のタイブレーク)。
type event struct {
	time int64
	seq  uint64
	fn   func()
}

type eventHeap []event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].time != h[j].time {
		return h[i].time < h[j].time
	}
	return h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x interface{}) { *h = append(*h, x.(event)) }
func (h *eventHeap) Pop() interface{} {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = event{}
	*h = old[:n-1]
	return e
}

// Queue は仮想時間で駆動される決定論的イベントキュー。
type Queue struct {
	h   eventHeap
	seq uint64
}

// At は仮想時刻 t に fn を実行するイベントを登録する。
func (q *Queue) At(t int64, fn func()) {
	q.seq++
	heap.Push(&q.h, event{time: t, seq: q.seq, fn: fn})
}

// Pop は次のイベントを取り出す。空なら ok=false。
func (q *Queue) Pop() (t int64, fn func(), ok bool) {
	if len(q.h) == 0 {
		return 0, nil, false
	}
	e := heap.Pop(&q.h).(event)
	return e.time, e.fn, true
}

// PeekTime は次のイベントの時刻を取り出さずに返す。
func (q *Queue) PeekTime() (int64, bool) {
	if len(q.h) == 0 {
		return 0, false
	}
	return q.h[0].time, true
}

// Len は残イベント数。
func (q *Queue) Len() int { return len(q.h) }
