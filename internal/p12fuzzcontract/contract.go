// Package p12fuzzcontract defines the immutable P12 fuzz execution contract.
package p12fuzzcontract

import (
	"fmt"
	"time"
)

const (
	Toolchain         = "go1.27.1"
	SmokeProfile      = "smoke"
	CertifyingProfile = "certifying"
	SmokeBudget       = 60 * time.Second
	CertifyingBudget  = 10 * time.Minute
)

type Target struct {
	Package string `json:"package"`
	Name    string `json:"name"`
}

var targets = [...]Target{
	{Package: ".", Name: "FuzzParseConfig"},
	{Package: ".", Name: "FuzzParseArgs"},
	{Package: "./internal/cephx", Name: "FuzzParseServerChallenge"},
	{Package: "./internal/cephx", Name: "FuzzParseKey"},
	{Package: "./internal/cephx", Name: "FuzzParseKeyring"},
	{Package: "./internal/cephx", Name: "FuzzParseAuthSessionReply"},
	{Package: "./internal/cephx", Name: "FuzzVerifyAuthorizerReply"},
	{Package: "./internal/cephx", Name: "FuzzAddAuthorizerChallenge"},
	{Package: "./internal/crush", Name: "FuzzDecodeMap"},
	{Package: "./internal/crush", Name: "FuzzDecodeAndPlace"},
	{Package: "./internal/encoding", Name: "FuzzDecoder"},
	{Package: "./internal/encoding", Name: "FuzzVersionedEnvelope"},
	{Package: "./internal/maps", Name: "FuzzDecodeMonMap"},
	{Package: "./internal/maps", Name: "FuzzDecodeOSDMap"},
	{Package: "./internal/maps", Name: "FuzzDecodeOSDMapIncremental"},
	{Package: "./internal/msgr", Name: "FuzzBanner"},
	{Package: "./internal/msgr", Name: "FuzzCRCFrame"},
	{Package: "./internal/msgr", Name: "FuzzSecureFrame"},
	{Package: "./internal/msgr", Name: "FuzzControlPayload"},
	{Package: "./internal/msgr", Name: "FuzzMessageFrame"},
	{Package: "./internal/msgr", Name: "FuzzSessionScript"},
	{Package: "./internal/osd", Name: "FuzzMetadataDecoders"},
	{Package: "./internal/osd", Name: "FuzzDecodeBackoff"},
	{Package: "./internal/osd", Name: "FuzzDecodeWatchNotification"},
	{Package: "./internal/osd", Name: "FuzzDecodeNotifyResult"},
	{Package: "./internal/osd", Name: "FuzzDecodeWatchers"},
	{Package: "./internal/osd", Name: "FuzzDecodeReply"},
	{Package: "./internal/osd", Name: "FuzzDecodePGNLSPage"},
	{Package: "./internal/osd", Name: "FuzzDecodeLockHolders"},
	{Package: "./internal/protocol", Name: "FuzzEntityAddr"},
	{Package: "./internal/protocol", Name: "FuzzEntityAddrVec"},
}

func Targets() []Target {
	return append([]Target(nil), targets[:]...)
}

func Budget(profile string) (time.Duration, bool) {
	switch profile {
	case SmokeProfile:
		return SmokeBudget, true
	case CertifyingProfile:
		return CertifyingBudget, true
	default:
		return 0, false
	}
}

func Command(target Target, budget time.Duration) []string {
	return []string{"go", "test", target.Package, "-run", "^$", "-fuzz", "^" + target.Name + "$", "-fuzztime=" + BudgetText(budget)}
}

func BudgetText(budget time.Duration) string {
	switch budget {
	case SmokeBudget:
		return "60s"
	case CertifyingBudget:
		return "10m"
	default:
		return fmt.Sprintf("%dns", budget.Nanoseconds())
	}
}