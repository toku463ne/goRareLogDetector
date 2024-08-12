package main

import (
	"os"
	"testing"
)

func Test_main1(t *testing.T) {
	logPathRegex := "../../test/data/rarelogdetector/analyzer/sample.log"
	os.Args = []string{"-m", "detect", "-f", logPathRegex, "-n", "1"}
	main()
}

func Test_main2(t *testing.T) {
	logPathRegex := "/home/ubuntu/logandata/openvpn_20240724/pfsense67051_openvpn.log*"
	os.Args = []string{"-m", "detect", "-f", logPathRegex, "-n", "10"}
	main()
}
