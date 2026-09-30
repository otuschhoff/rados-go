package perfbaseline

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strconv"
)

var nativeReady = regexp.MustCompile(`--2- .* >> .* conn\((0x[[:xdigit:]]+) (0x[[:xdigit:]]+) ([^ ]+) :[^)]* s=READY [^)]*\)\.[ \t]*ready entity=(mon|osd)\.[^ \t]+`)
var nativeRelevantReady = regexp.MustCompile(`\bready entity=(mon|osd)\.[^ \t]+`)

func CaptureNativeModes(reader io.Reader, requested string) (ModeEvidence, error) {
	evidence := ModeEvidence{Implementation: "native", Requested: requested, Connections: []ConnectionMode{}}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		if !nativeRelevantReady.MatchString(line) {
			continue
		}
		match := nativeReady.FindStringSubmatch(line)
		if match == nil {
			return evidence, fmt.Errorf("unsupported native ready record at line %d", lineNumber)
		}
		service := "monitor"
		if match[4] == "osd" {
			service = "osd"
		}
		evidence.Connections = append(evidence.Connections, ConnectionMode{
			Service: service, Connection: match[1] + "/" + match[2] + "/line-" + strconv.Itoa(lineNumber), Actual: match[3], Source: "ceph-ready-log",
		})
	}
	return evidence, scanner.Err()
}
