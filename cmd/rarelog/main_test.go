package main

import (
	"fmt"
	"goRareLogDetector/internal/rarelogdetector"
	"goRareLogDetector/pkg/utils"
	"os"
	"testing"
)

func Test_main1(t *testing.T) {
	logPathRegex := "../../test/data/rarelogdetector/analyzer/sample.log"
	os.Args = []string{"rarelog", "-m", "detect", "-f", logPathRegex}
	main()
}

func Test_main_config(t *testing.T) {
	rootDir, err := utils.InitTestDir("Test_main_config")
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	configPath := "../../test/data/rarelogdetector/yaml/config_test1.yaml"
	os.Args = []string{"rarelog", "-m", "feed", "-c", configPath}
	main()

	dataDir := fmt.Sprintf("%s/data", rootDir)
	a, err := rarelogdetector.NewAnalyzer2(dataDir, searchStrings, excludeStrings, true)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	res, err := a.TopN(5, 10, 7, false, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("topN len", len(res), 3); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("topN 1", res[0].Count, 1); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("topN 1", res[1].Count, 7); err != nil {
		t.Errorf("%v", err)
		return
	}

}

func Test_real(t *testing.T) {
	//conf := "/home/ubuntu/logandata/openvpn.yml"
	conf := "/home/administrator/tests/openvpn.yaml"
	os.Args = []string{"rarelog", "-m", "feed", "-c", conf}
	main()
}

func Test_real2(t *testing.T) {
	os.Args = []string{"rarelog", "-d", "/home/ubuntu/logandata/openvpn_data", "-x", "username attempted to change from"}
	main()
}

func Test_real_sugcap(t *testing.T) {
	//conf := "/home/ubuntu/logandata/openvpn.yml"
	conf := "/home/administrator/tests/sugcap/sugcap.yml"
	os.Args = []string{"rarelog", "-m", "feed", "-c", conf}
	main()
}

func Test_real_sugcap2(t *testing.T) {
	//conf := "/home/ubuntu/logandata/openvpn.yml"
	conf := "/home/administrator/tests/sugcap/sugcap.yml"
	os.Args = []string{"rarelog", "-m", "showPhrases", "-c", conf}
	main()
}
