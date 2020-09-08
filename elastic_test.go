package ss_elastic_store

import (
	"fmt"
	logger "github.com/ipfs/go-log/v2"
	"net/http"

	testsuite "github.com/StreamSpace/ss-store/testsuite"
	"testing"
)

func TestElastic(t *testing.T) {
	logger.SetLogLevel("*", "Debug")
	es := &ElasticStoreConfig{
		Url:       "http://localhost:9200",
		Index:     "elasticdb",
		IndexType: "document",
	}
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/%s", es.Url, es.Index), nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer resp.Body.Close()

	logger.SetLogLevel("*", "Debug")
	elastconf, err := NewElasticStore(es)
	if err != nil {
		t.Errorf(err.Error())
	}
	testsuite.RunTestsuite(t, elastconf, testsuite.Basic)
}
