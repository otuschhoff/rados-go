package rados

import "testing"

func FuzzParseConfig(f *testing.F) {
	f.Add([]byte("[global]\nentity=client.test\nmon_host=127.0.0.1\n"))
	f.Add([]byte("[global]\ninclude=/tmp/other.conf\n"))
	f.Add([]byte("[global]\nmax_sessions=256\nmax_receive_bytes=268435456\nmax_queued_receive_bytes=67108864\n"))
	f.Add([]byte("[global]\nmax_receive_bytes=18446744073709551616\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ParseConfig(data)
	})
}

func FuzzParseArgs(f *testing.F) {
	f.Add("--name=client.test\x00--mon-host\x00127.0.0.1")
	f.Add("input\x00--unknown\x00value")
	f.Add("--max-sessions=256\x00--max-receive-bytes\x00268435456\x00--max-queued-receive-bytes=67108864")
	f.Add("--max-sessions=-1\x00--max-receive-bytes=18446744073709551616")
	f.Fuzz(func(t *testing.T, encoded string) {
		arguments := splitFuzzArguments(encoded)
		_, _, _ = DefaultConfig().ParseArgs(arguments)
	})
}

func splitFuzzArguments(encoded string) []string {
	arguments := make([]string, 0, 16)
	start := 0
	for index := 0; index <= len(encoded) && len(arguments) < 512; index++ {
		if index == len(encoded) || encoded[index] == 0 {
			arguments = append(arguments, encoded[start:index])
			start = index + 1
		}
	}
	return arguments
}
