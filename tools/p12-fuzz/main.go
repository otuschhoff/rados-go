// Command p12-fuzz runs and records the exact P12 fuzz target contract.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/otuschhoff/rados-go/internal/p12fuzzcontract"
	"github.com/otuschhoff/rados-go/internal/p12fuzzevidence"
)

func main() {
	profile := flag.String("profile", "", "fuzz profile: smoke or certifying")
	root := flag.String("root", ".", "repository root")
	out := flag.String("out", p12fuzzevidence.ReportPath, "report path relative to root")
	pending := flag.Bool("record-pending", false, "record truthful not-run evidence and exit nonzero")
	flag.Parse()
	if flag.NArg() != 0 {
		fatalf("unexpected positional arguments")
	}
	budget, ok := p12fuzzcontract.Budget(*profile)
	if !ok {
		fatalf("profile must be smoke or certifying")
	}
	if runtime.Version() != p12fuzzcontract.Toolchain {
		fatalf("runner requires exact runtime %s, got %s", p12fuzzcontract.Toolchain, runtime.Version())
	}
	if runtime.GOOS != "darwin" {
		fatalf("runner requires a darwin host, got %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	passed, err := run(ctx, *root, *out, *profile, budget, *pending)
	if err != nil {
		fatalf("P12 fuzz: %v", err)
	}
	if !passed {
		os.Exit(1)
	}
}

func run(ctx context.Context, root, output, profile string, budget time.Duration, pending bool) (bool, error) {
	return runWithClock(ctx, root, output, profile, budget, pending, time.Now)
}

func runWithClock(ctx context.Context, root, output, profile string, budget time.Duration, pending bool, now func() time.Time) (bool, error) {
	started := now().UTC()
	platform := runtime.GOOS + "/" + runtime.GOARCH
	source, err := p12fuzzevidence.SourceArtifacts(root)
	if err != nil {
		return false, err
	}
	report := p12fuzzevidence.Report{
		SchemaVersion: 1, Status: "passed", Profile: profile,
		Command:   p12fuzzevidence.Command + " -profile " + profile,
		Toolchain: p12fuzzcontract.Toolchain, Platform: platform,
		StartedAt: p12fuzzevidence.Timestamp(started),
		Source:    p12fuzzevidence.Source{Identity: "content-addressed-artifacts", Artifacts: source},
	}
	if pending {
		report.Command += " -record-pending"
	}
	targetStarted := started
	for _, target := range p12fuzzcontract.Targets() {
		arguments := p12fuzzcontract.Command(target, budget)
		result := p12fuzzevidence.TargetExecution{
			Package: target.Package, Name: target.Name, Budget: p12fuzzcontract.BudgetText(budget),
			Command:   "CGO_ENABLED=0 GOTOOLCHAIN=" + p12fuzzcontract.Toolchain + " " + strings.Join(arguments, " "),
			Toolchain: p12fuzzcontract.Toolchain, Platform: platform,
		}
		result.StartedAt = p12fuzzevidence.Timestamp(targetStarted)
		if pending || ctx.Err() != nil {
			result.ExitStatus = -1
			if pending {
				result.Output = "not run: P12 fuzz evidence is pending\n"
			} else {
				result.Output = "not run: P12 fuzz execution was interrupted\n"
			}
		} else {
			command := exec.CommandContext(ctx, arguments[0], arguments[1:]...)
			command.Dir = root
			command.Env = append(os.Environ(), "CGO_ENABLED=0", "GOTOOLCHAIN="+p12fuzzcontract.Toolchain)
			output, runErr := command.CombinedOutput()
			result.Output = string(output)
			result.ExitStatus = exitStatus(runErr)
		}
		targetFinished := now().UTC()
		result.FinishedAt = p12fuzzevidence.Timestamp(targetFinished)
		result.Pass = result.ExitStatus == 0
		result.OutputSHA256 = p12fuzzevidence.HashOutput(result.Output)
		report.Targets = append(report.Targets, result)
		if !result.Pass {
			report.Status = "failed"
		}
		targetStarted = targetFinished
	}
	report.FinishedAt = p12fuzzevidence.Timestamp(now().UTC())
	path := output
	if !strings.HasPrefix(output, "/") {
		path = root + "/" + output
	}
	if err := p12fuzzevidence.Write(path, report); err != nil {
		return false, err
	}
	return report.Status == "passed", nil
}

func exitStatus(err error) int {
	if err == nil {
		return 0
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return exitError.ExitCode()
	}
	return 1
}

func fatalf(format string, values ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(1)
}
