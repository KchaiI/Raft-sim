package checker

import "testing"

// 線形化チェッカー自身の単体検証: 既知の合法/違法履歴ベクタ (SPEC §4.2)。

func get(c uint64, key, val string, ok bool, inv, resp int64) LinOp {
	return LinOp{ClientID: c, Kind: LinGet, Key: key, OutValue: val, OutOK: ok, Invoke: inv, Response: resp, Complete: true}
}
func put(c uint64, key, val string, inv, resp int64) LinOp {
	return LinOp{ClientID: c, Kind: LinPut, Key: key, Arg1: val, OutOK: true, Invoke: inv, Response: resp, Complete: true}
}
func cas(c uint64, key, expect, newv string, ok bool, curOnFail string, inv, resp int64) LinOp {
	return LinOp{ClientID: c, Kind: LinCAS, Key: key, Arg1: expect, Arg2: newv, OutOK: ok, OutValue: curOnFail, Invoke: inv, Response: resp, Complete: true}
}
func pendingPut(c uint64, key, val string, inv int64) LinOp {
	return LinOp{ClientID: c, Kind: LinPut, Key: key, Arg1: val, Invoke: inv, Complete: false}
}

func legal(t *testing.T, name string, ops []LinOp) {
	t.Helper()
	if err := CheckLinearizable(ops); err != nil {
		t.Errorf("%s: 合法履歴が違反と判定された: %v", name, err)
	}
}
func illegal(t *testing.T, name string, ops []LinOp) {
	t.Helper()
	if err := CheckLinearizable(ops); err == nil {
		t.Errorf("%s: 違法履歴が合法と判定された", name)
	}
}

func TestLegalHistories(t *testing.T) {
	legal(t, "空履歴", nil)
	legal(t, "逐次 Put→Get", []LinOp{
		put(1, "k", "a", 0, 10),
		get(1, "k", "a", true, 20, 30),
	})
	legal(t, "未書き込みキーの Get", []LinOp{
		get(1, "k", "", false, 0, 10),
	})
	legal(t, "並行 Put/Get で古い値を読む", []LinOp{
		put(1, "k", "a", 0, 10),
		put(2, "k", "b", 20, 40), // Get と並行
		get(3, "k", "a", true, 25, 35),
	})
	legal(t, "並行 Put/Get で新しい値を読む", []LinOp{
		put(1, "k", "a", 0, 10),
		put(2, "k", "b", 20, 40),
		get(3, "k", "b", true, 25, 35),
	})
	legal(t, "CAS 成功チェーン", []LinOp{
		put(1, "k", "v0", 0, 10),
		cas(2, "k", "v0", "v1", true, "", 20, 30),
		cas(3, "k", "v1", "v2", true, "", 40, 50),
		get(1, "k", "v2", true, 60, 70),
	})
	legal(t, "CAS 失敗が現在値を観測", []LinOp{
		put(1, "k", "x", 0, 10),
		cas(2, "k", "y", "z", false, "x", 20, 30),
	})
	legal(t, "並行 CAS は一方だけ成功", []LinOp{
		put(1, "k", "v0", 0, 10),
		cas(2, "k", "v0", "a", true, "", 20, 40),
		cas(3, "k", "v0", "b", false, "a", 25, 45),
	})
	legal(t, "未完了 Put が効いた世界", []LinOp{
		pendingPut(1, "k", "a", 0),
		get(2, "k", "a", true, 10, 20),
	})
	legal(t, "未完了 Put が効かなかった世界", []LinOp{
		pendingPut(1, "k", "a", 0),
		get(2, "k", "", false, 10, 20),
	})
	legal(t, "別キーは独立", []LinOp{
		put(1, "k1", "a", 0, 10),
		put(2, "k2", "b", 5, 15),
		get(1, "k2", "b", true, 20, 30),
		get(2, "k1", "a", true, 20, 30),
	})
	// 実時間順序の重要ケース: 区間が重なる 3 操作
	legal(t, "3-way 並行", []LinOp{
		put(1, "k", "a", 0, 100),
		put(2, "k", "b", 10, 90),
		get(3, "k", "a", true, 20, 80),
	})
}

func TestIllegalHistories(t *testing.T) {
	illegal(t, "完了した Put を無視した Get", []LinOp{
		put(1, "k", "a", 0, 10),
		get(2, "k", "", false, 20, 30), // Put 完了後に「存在しない」は違法
	})
	illegal(t, "stale read (完了後に古い値)", []LinOp{
		put(1, "k", "a", 0, 10),
		put(2, "k", "b", 20, 30),
		get(3, "k", "a", true, 40, 50), // b の Put 完了後に a は違法
	})
	illegal(t, "読み戻り (時間逆行)", []LinOp{
		put(1, "k", "a", 0, 10),
		put(1, "k", "b", 20, 30),
		get(2, "k", "b", true, 40, 50),
		get(2, "k", "a", true, 60, 70), // b を読んだ後に a へ戻る
	})
	illegal(t, "存在しない値を読む", []LinOp{
		put(1, "k", "a", 0, 10),
		get(2, "k", "zzz", true, 20, 30),
	})
	illegal(t, "逐次 CAS が両方成功", []LinOp{
		put(1, "k", "v0", 0, 10),
		cas(2, "k", "v0", "a", true, "", 20, 30),
		cas(3, "k", "v0", "b", true, "", 40, 50), // v0 はもう存在しない
	})
	illegal(t, "CAS 成功なのに Get が旧値 (完了後)", []LinOp{
		put(1, "k", "v0", 0, 10),
		cas(2, "k", "v0", "v1", true, "", 20, 30),
		get(3, "k", "v0", true, 40, 50),
	})
	illegal(t, "CAS 失敗の観測値が不整合", []LinOp{
		put(1, "k", "x", 0, 10),
		cas(2, "k", "y", "z", false, "傍観者にない値", 20, 30),
	})
	illegal(t, "未完了 Put でも説明できない値", []LinOp{
		pendingPut(1, "k", "a", 0),
		get(2, "k", "b", true, 10, 20),
	})
	illegal(t, "同一クライアントのプログラム順序違反", []LinOp{
		put(1, "k", "a", 0, 10),
		put(1, "k", "b", 20, 30),
		get(1, "k", "a", true, 40, 50),
	})
}

// 大きめの合法履歴で探索が終わること (メモ化の煙テスト)。
func TestLargeSequentialHistory(t *testing.T) {
	var ops []LinOp
	tick := int64(0)
	val := ""
	for i := 0; i < 300; i++ {
		v := string(rune('a' + i%26))
		ops = append(ops, put(uint64(i%5+1), "k", v, tick, tick+5))
		tick += 10
		ops = append(ops, get(uint64(i%3+1), "k", v, true, tick, tick+5))
		tick += 10
		val = v
	}
	_ = val
	legal(t, "300 対の逐次 Put/Get", ops)
}
