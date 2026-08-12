// Command verify-history checks one or more recorded operation histories
// (etcfuse-meta --history-log) against the consistency models in
// test/verify. See docs/verification/porcupine.md.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/MHS-20/EtcFS/internal/history"
	"github.com/MHS-20/EtcFS/test/verify"
)

func main() {
	var files, models string
	var timeout time.Duration
	flag.StringVar(&files, "files", "", "comma-separated history files, one per node")
	flag.StringVar(&models, "models", "namespace,extent,lock,generation",
		"comma-separated models to check: namespace, extent, lock, generation")
	flag.DurationVar(&timeout, "timeout", 5*time.Minute, "per-model checker timeout")
	flag.Parse()

	if files == "" {
		fmt.Fprintln(os.Stderr, "usage: verify-history --files=history-n1.jsonl,history-n2.jsonl [--models=namespace,extent,lock,generation]")
		os.Exit(2)
	}

	var entries []history.Entry
	for _, path := range strings.Split(files, ",") {
		e, err := history.Load(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load %s: %v\n", path, err)
			os.Exit(1)
		}
		fmt.Printf("%s: %d entries\n", path, len(e))
		entries = append(entries, e...)
	}

	failed := false
	for _, model := range strings.Split(models, ",") {
		ok, err := checkModel(strings.TrimSpace(model), entries, timeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", model, err)
			failed = true
			continue
		}
		if !ok {
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
}

func checkModel(model string, entries []history.Entry, timeout time.Duration) (bool, error) {
	switch model {
	case "namespace":
		ops, err := verify.DecodeNamespace(entries)
		if err != nil {
			return false, err
		}
		fmt.Printf("namespace: checking %d operations\n", len(ops))
		return report("namespace", verify.Check(verify.NamespaceModel, verify.Operations(ops), verify.AllLinearizable, 0, timeout))
	case "extent":
		ops, err := verify.DecodeExtents(entries)
		if err != nil {
			return false, err
		}
		fmt.Printf("extent: checking %d operations\n", len(ops))
		return report("extent", verify.CheckExtents(ops, timeout))
	case "lock":
		ops, err := verify.DecodeLocks(entries)
		if err != nil {
			return false, err
		}
		fmt.Printf("lock: checking %d events\n", len(ops))
		return report("lock", verify.CheckLocks(ops, timeout))
	case "generation":
		ops, err := verify.DecodeGuardedCommits(entries)
		if err != nil {
			return false, err
		}
		fmt.Printf("generation: checking %d commits\n", len(ops))
		return report("generation", verify.CheckGenerations(ops, timeout))
	}
	return false, fmt.Errorf("unknown model %q", model)
}

func report(name string, res porcupine.CheckResult) (bool, error) {
	switch res {
	case porcupine.Ok:
		fmt.Printf("%s: OK\n", name)
		return true, nil
	case porcupine.Unknown:
		fmt.Printf("%s: TIMED OUT\n", name)
		return false, nil
	default:
		fmt.Printf("%s: VIOLATION\n", name)
		return false, nil
	}
}
