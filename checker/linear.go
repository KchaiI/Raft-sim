package checker

import (
	"fmt"
	"math"
	"sort"
)

// 線形化可能性チェッカー (SPEC §4.2)。
// Wing & Gong アルゴリズム (WGL) を、(linearized 集合, レジスタ状態) の
// メモ化つき DFS として実装し、P-compositionality (キーごとの分割検査:
// 各操作は単一キーにのみ触れるため、全体が線形化可能 ⟺ 各キーの射影が
// 線形化可能) で分割する。論文: Wing & Gong 1993, Horn & Kroening 2015。

// LinOp の Kind。
const (
	LinGet uint8 = iota
	LinPut
	LinCAS
)

// LinOp は履歴中の 1 操作 (invoke/response の実時間区間 + 結果)。
type LinOp struct {
	ClientID uint64
	Kind     uint8
	Key      string
	Arg1     string // Put: 書く値 / CAS: 期待値
	Arg2     string // CAS: 新値
	OutValue string // Get: 読んだ値 / CAS 失敗: 観測した現在値
	OutOK    bool   // Get: キー存在 / CAS: 成功
	Invoke   int64
	Response int64
	Complete bool // false = 応答を受けていない (効果があったかは不明)
}

func (o LinOp) String() string {
	kind := [...]string{"Get", "Put", "CAS"}[o.Kind]
	return fmt.Sprintf("c%d %s(%q,%q,%q)=(%q,%v) [%d,%d] complete=%v",
		o.ClientID, kind, o.Key, o.Arg1, o.Arg2, o.OutValue, o.OutOK, o.Invoke, o.Response, o.Complete)
}

// regState は単一キーのレジスタ状態。
type regState struct {
	value   string
	present bool
}

// CheckLinearizable は履歴全体を検査する。違反があればそのキーを含む error。
func CheckLinearizable(ops []LinOp) error {
	byKey := map[string][]LinOp{}
	for _, o := range ops {
		byKey[o.Key] = append(byKey[o.Key], o)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys) // 決定論のため
	for _, k := range keys {
		if !checkKey(byKey[k]) {
			return fmt.Errorf("線形化可能性違反: key %q (操作数 %d)", k, len(byKey[k]))
		}
	}
	return nil
}

// applyOp は状態遷移と「その線形化位置で返るべき出力」を計算する。
func applyOp(st regState, o *LinOp) (next regState, outValue string, outOK bool) {
	switch o.Kind {
	case LinGet:
		return st, st.value, st.present
	case LinPut:
		return regState{value: o.Arg1, present: true}, "", true
	case LinCAS:
		if st.present && st.value == o.Arg1 {
			return regState{value: o.Arg2, present: true}, "", true
		}
		return st, st.value, false
	}
	panic("unknown op kind")
}

// outputMatches は完了済み操作の記録された出力と一致するか。
func outputMatches(o *LinOp, outValue string, outOK bool) bool {
	switch o.Kind {
	case LinGet:
		return o.OutOK == outOK && o.OutValue == outValue
	case LinPut:
		return true
	case LinCAS:
		if o.OutOK != outOK {
			return false
		}
		if !outOK {
			return o.OutValue == outValue // 失敗時に観測した現在値も一致すること
		}
		return true
	}
	return false
}

// checkKey は単一キーの履歴が線形化可能かを WGL で判定する。
//
// 探索: 「次に線形化する操作」は、未線形化の完了済み操作の最小 response 時刻
// より前に invoke されていなければならない (そうでなければ、その response が
// 先に完了した操作が順序上先行する)。未完了操作は任意の時点で線形化してよく、
// 線形化しなくてもよい (クラッシュで失われた可能性)。
// 全ての完了済み操作を線形化できれば合法。
func checkKey(ops []LinOp) bool {
	sort.SliceStable(ops, func(i, j int) bool { return ops[i].Invoke < ops[j].Invoke })
	n := len(ops)
	if n == 0 {
		return true
	}

	linearized := make([]bool, n)
	maskBytes := make([]byte, (n+7)/8)
	remaining := 0
	for i := range ops {
		if ops[i].Complete {
			remaining++
		}
	}
	visited := map[string]struct{}{}
	st := regState{}
	nodes := 0

	var dfs func() bool
	dfs = func() bool {
		if remaining == 0 {
			return true
		}
		nodes++
		if nodes > 20_000_000 {
			// 健全性のため「不明」を違反側に倒さない: 探索爆発は設計上
			// 起こらない想定であり、起きたら再現可能なシードで調査する
			panic(fmt.Sprintf("線形化チェッカーの探索爆発: ops=%d", n))
		}
		memoKey := string(maskBytes) + "\x00" + st.value + "\x00" + map[bool]string{true: "1", false: "0"}[st.present]
		if _, ok := visited[memoKey]; ok {
			return false
		}
		visited[memoKey] = struct{}{}

		minResp := int64(math.MaxInt64)
		for i := range ops {
			if !linearized[i] && ops[i].Complete && ops[i].Response < minResp {
				minResp = ops[i].Response
			}
		}
		for i := range ops {
			if linearized[i] {
				continue
			}
			o := &ops[i]
			if o.Invoke > minResp {
				break // invoke 昇順ソート済み: 以降の操作はすべて候補外
			}
			next, outV, outOK := applyOp(st, o)
			if o.Complete && !outputMatches(o, outV, outOK) {
				continue
			}
			linearized[i] = true
			maskBytes[i/8] |= 1 << (i % 8)
			if o.Complete {
				remaining--
			}
			old := st
			st = next
			if dfs() {
				return true
			}
			st = old
			linearized[i] = false
			maskBytes[i/8] &^= 1 << (i % 8)
			if o.Complete {
				remaining++
			}
		}
		return false
	}
	return dfs()
}
