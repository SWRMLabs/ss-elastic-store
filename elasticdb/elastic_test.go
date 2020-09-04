package elasticdb

import (
	"net/http"

	testsuite "github.com/StreamSpace/ss-store/testsuite"
	logger "github.com/ipfs/go-log/v2"
	"testing"
)

func TestElastic(t *testing.T) {

	es := &elasticStore{
		Url:       "http://localhost:9200",
		Index:     "elasticdb",
		IndexType: "document",
	}
	req, err := http.NewRequest("DELETE", "http://localhost:9200/elasticdb", nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer resp.Body.Close()

	logger.SetLogLevel("*", "Debug")
	elastconf, err := ElasticConfig(es)
	if err != nil {
		t.Errorf(err.Error())
	}
	testsuite.RunTestsuite(t, elastconf, testsuite.Advanced)
}
