GO ?= go

.PHONY: all test vet check-imports verify soak scenarios coverage build clean

all: verify

build:
	$(GO) build ./...

vet:
	$(GO) vet ./...

# SPEC §6: Raft コアに time / math/rand / sync を import しない (機械検査)
check-imports:
	$(GO) run ./tools/importcheck raft

test:
	$(GO) test ./...

verify: vet check-imports test
	@echo "verify OK"

clean:
	rm -f coverage.out
