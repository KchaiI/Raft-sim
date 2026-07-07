GO ?= go
SOAK_SEEDS ?= 10000

.PHONY: all build vet test check-imports check-deps coverage soak verify clean

all: verify

build:
	$(GO) build ./...

vet:
	$(GO) vet ./...

# SPEC §6: Raft コアに time / math/rand / sync を import しない (機械検査)
check-imports:
	$(GO) run ./tools/importcheck raft

# SPEC §4.4: 外部依存ゼロ (go.mod の require が空 = モジュールが自分自身のみ)
check-deps:
	@[ "$$($(GO) list -m all)" = "raftsim" ] && echo "check-deps OK: 外部依存ゼロ" \
		|| { echo "外部依存が存在する:"; $(GO) list -m all; exit 1; }

test:
	$(GO) test ./... -timeout 1800s

# 全テスト (狙い撃ちシナリオスイート・線形化チェッカー自己テスト込み) を
# Raft コアのカバレッジ計測つきで実行し、90% 以上を機械検査する (SPEC §4.4)
coverage:
	$(GO) test ./... -timeout 1800s -coverpkg=./raft -coverprofile=coverage.out
	@total=$$($(GO) tool cover -func=coverage.out | tail -1 | awk '{gsub(/%/,"",$$3); print $$3}'); \
	 awk -v t=$$total 'BEGIN { if (t+0 < 90.0) { print "NG: raft コアのカバレッジ " t "% < 90%"; exit 1 } \
	                           else { print "coverage OK: raft コア " t "% (>= 90%)" } }'

# SPEC §4.4: 10,000 シード × 3/5/7 ノード混合 × 全障害有効、違反ゼロ
soak:
	$(GO) run ./cmd/raftsim soak -seeds $(SOAK_SEEDS)

# SPEC §4.4 Definition of Done の全検証
verify: vet check-imports check-deps coverage soak
	@echo ""
	@echo "==== make verify: すべて通過 (SPEC §4.4 Definition of Done) ===="

clean:
	rm -f coverage.out
