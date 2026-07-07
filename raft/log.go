package raft

import "fmt"

// raftLog はスナップショット以降のログを保持する。
// entries[i].Index == snapIndex + 1 + i の不変条件を維持する。
type raftLog struct {
	snapIndex uint64
	snapTerm  uint64
	entries   []Entry
}

func (l *raftLog) firstIndex() uint64 { return l.snapIndex + 1 }

func (l *raftLog) lastIndex() uint64 { return l.snapIndex + uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 {
	if len(l.entries) == 0 {
		return l.snapTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// term は index i のエントリの term。スナップショット地点 (snapIndex) も返せる。
// 範囲外 (圧縮済み・未到達) は ok=false。
func (l *raftLog) term(i uint64) (uint64, bool) {
	if i == l.snapIndex {
		return l.snapTerm, true
	}
	if i < l.snapIndex || i > l.lastIndex() {
		return 0, false
	}
	return l.entries[i-l.snapIndex-1].Term, true
}

func (l *raftLog) entry(i uint64) *Entry {
	if i < l.firstIndex() || i > l.lastIndex() {
		return nil
	}
	return &l.entries[i-l.snapIndex-1]
}

// slice は [lo, hi) のエントリを新しいスライスにコピーして返す。
// backing array の共有によるエイリアシング破壊を防ぐ (NOTES.md)。
func (l *raftLog) slice(lo, hi uint64, maxEntries int) []Entry {
	if hi > l.lastIndex()+1 {
		hi = l.lastIndex() + 1
	}
	if lo < l.firstIndex() || lo >= hi {
		return nil
	}
	n := int(hi - lo)
	if maxEntries > 0 && n > maxEntries {
		n = maxEntries
	}
	out := make([]Entry, n)
	copy(out, l.entries[lo-l.snapIndex-1:])
	return out
}

// append は末尾に追記する。e.Index == lastIndex()+1 でなければ panic
// (呼び出し側のバグを早期に暴く)。
func (l *raftLog) append(ents ...Entry) {
	for _, e := range ents {
		if e.Index != l.lastIndex()+1 {
			panic(fmt.Sprintf("raftLog.append: index %d != lastIndex+1 %d", e.Index, l.lastIndex()+1))
		}
		l.entries = append(l.entries, e)
	}
}

// truncateFrom は index i 以降のエントリを削除する。
func (l *raftLog) truncateFrom(i uint64) {
	if i <= l.snapIndex {
		panic(fmt.Sprintf("raftLog.truncateFrom: %d はスナップショット済み領域", i))
	}
	if i > l.lastIndex() {
		return
	}
	l.entries = l.entries[:i-l.snapIndex-1]
}

// compact は index i 以前のエントリを破棄しスナップショット地点を進める。
func (l *raftLog) compact(i, term uint64) {
	if i <= l.snapIndex {
		return
	}
	if i > l.lastIndex() {
		panic(fmt.Sprintf("raftLog.compact: %d > lastIndex %d", i, l.lastIndex()))
	}
	keep := l.entries[i-l.snapIndex:]
	l.entries = make([]Entry, len(keep))
	copy(l.entries, keep)
	l.snapIndex = i
	l.snapTerm = term
}

// reset はスナップショットでログ全体を置き換える。
func (l *raftLog) reset(snapIndex, snapTerm uint64) {
	l.snapIndex = snapIndex
	l.snapTerm = snapTerm
	l.entries = nil
}

// firstIndexOfTerm は term t を持つ最初のエントリの index を後方走査で求める
// (conflict 高速バックアップ用)。from はその term を持つことが既知の index。
func (l *raftLog) firstIndexOfTerm(from uint64, t uint64) uint64 {
	i := from
	for i > l.firstIndex() {
		pt, ok := l.term(i - 1)
		if !ok || pt != t {
			break
		}
		i--
	}
	return i
}

// lastIndexOfTerm は term t を持つ最後のエントリの index (なければ 0)。
func (l *raftLog) lastIndexOfTerm(t uint64) uint64 {
	for i := l.lastIndex(); i >= l.firstIndex(); i-- {
		pt, _ := l.term(i)
		if pt == t {
			return i
		}
		if pt < t {
			return 0
		}
	}
	return 0
}
