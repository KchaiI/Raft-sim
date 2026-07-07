// importcheck は Raft コアパッケージの禁止 import を機械検査する (SPEC §6)。
// raft/ 配下の全 .go ファイル(テスト含む)を go/parser で解析し、
// time / math/rand / math/rand/v2 / sync / sync/atomic / os / net 系の
// import があれば非ゼロ終了する。DECISIONS.md D-002 参照。
package main

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var forbidden = []string{
	"time",
	"math/rand",
	"math/rand/v2",
	"sync",
	"sync/atomic",
	"os",
	"net",
}

func isForbidden(path string) bool {
	for _, f := range forbidden {
		if path == f || strings.HasPrefix(path, f+"/") {
			return true
		}
	}
	return false
}

func main() {
	dirs := os.Args[1:]
	if len(dirs) == 0 {
		dirs = []string{"raft"}
	}
	bad := 0
	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if err != nil {
				return fmt.Errorf("parse %s: %w", path, err)
			}
			for _, imp := range f.Imports {
				p, _ := strconv.Unquote(imp.Path.Value)
				if isForbidden(p) {
					fmt.Fprintf(os.Stderr, "FORBIDDEN IMPORT: %s imports %q\n", path, p)
					bad++
				}
			}
			return nil
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, "importcheck:", err)
			os.Exit(2)
		}
	}
	if bad > 0 {
		os.Exit(1)
	}
	fmt.Printf("importcheck OK: %v に禁止 import なし\n", dirs)
}
