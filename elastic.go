package ss_elastic_store

import (
	"context"
	"encoding/json"
	"errors"
	store "github.com/StreamSpace/ss-store"
	logger "github.com/ipfs/go-log/v2"
	elastic "github.com/olivere/elastic/v7"
	"strings"
)

var log = logger.Logger("store/elastic")

type ElasticStore struct {
	Url       string
	Index     string
	IndexType string
}

type SsElastic struct {
	eclient  *elastic.Client
	elconfig *ElasticStore
}

func ElasticConfig(config *ElasticStore) (*SsElastic, error) {
	eclient, err := elastic.NewClient(elastic.SetURL(config.Url))
	if err != nil {
		log.Errorf("Failed to create elastic client %s", err.Error())
		return nil, err
	}
	return &SsElastic{
		eclient:  eclient,
		elconfig: config,
	}, nil
}

func (e *SsElastic) Create(i store.Item) error {
	resp, err := e.eclient.IndexExists(e.elconfig.Index).Do(context.Background())
	if err != nil {
		log.Errorf("Failed check index existence %s", err.Error())
		return err
	}
	if !resp {
		_, err := e.eclient.CreateIndex(e.elconfig.Index).Do(context.Background())
		if err != nil {
			log.Errorf("Failed index creation %s", err.Error())
			return err
		}
	}
	jsondata, err := json.Marshal(i)
	if err != nil {
		log.Errorf("mrashal failed %s", err.Error())
	}

	_, err = e.eclient.Index().Index(e.elconfig.Index).Type(e.elconfig.IndexType).Id(i.GetId()).BodyJson(string(jsondata)).Do(context.Background())
	if err != nil {
		log.Errorf("Faild to add document in index %s", err.Error())
		return err
	}
	return nil
}

func (e *SsElastic) Read(i store.Item) error {
	seriallizable, ok := i.(store.Serializable)
	if ok != true {
		return errors.New("item is not serializable")
	}
	resp, err := e.eclient.IndexExists(e.elconfig.Index).Do(context.Background())
	if err != nil {
		log.Debugf("Failed check index existence %s", err.Error())
		return err
	}

	if !resp {
		return errors.New("Index not created , read data failed")
	}

	getdata, err := e.eclient.Get().Index(e.elconfig.Index).Type(e.elconfig.IndexType).Id(i.GetId()).Do(context.Background())
	if err != nil {
		log.Errorf("Failed to read data from db %s", err.Error())
		return err
	}
	err = seriallizable.Unmarshal(getdata.Source)
	if err != nil {
		log.Errorf("Failed to unmarshal received data %s", err.Error())
	}
	return nil
}

func (e *SsElastic) Delete(i store.Item) error {

	resp, err := e.eclient.IndexExists(e.elconfig.Index).Do(context.Background())
	if err != nil {
		log.Debugf("Failed check index existence %s", err.Error())
		return err
	}
	if !resp {
		return errors.New("Index not created , data deletion failed")
	}

	_, err = e.eclient.Delete().Index(e.elconfig.Index).Type(e.elconfig.IndexType).Id(i.GetId()).Do(context.Background())
	if err != nil {
		log.Errorf("Document Deletion failed %s", err.Error())
		return err
	}
	return nil
}

func (e *SsElastic) Update(i store.Item) error {
	resp, err := e.eclient.IndexExists(e.elconfig.Index).Do(context.Background())
	if err != nil {
		log.Debugf("Failed check index existence %s", err.Error())
		return err
	}
	if !resp {
		return errors.New("Index doesn't exist , data updation is failed")
	}

	id := i.GetId()

	_, err = e.eclient.Update().Index(e.elconfig.Index).Type(e.elconfig.IndexType).Id(id).Doc(i).Do(context.Background())
	if err != nil {
		log.Errorf("Document updation is failed %s", err.Error())
		return err
	}
	return nil
}

func (e *SsElastic) List(factory store.Factory, o store.ListOpt) (store.Items, error) {
	var (
		list = []store.Item{}
		skip = o.Page * o.Limit
	)
	resp, err := e.eclient.IndexExists(e.elconfig.Index).Do(context.Background())
	if err != nil {
		log.Errorf("Failed to check index existence %s", err.Error())
		return nil, err
	}
	if !resp {
		return nil, errors.New("Index doesn't exist , list operation is failed")
	}

	var query *elastic.BoolQuery
	query = elastic.NewBoolQuery().Must(elastic.NewTermQuery("Namespace", strings.ToLower(factory.Factory().GetNamespace())))
	if o.Filter != nil {
		return nil, errors.New("We don't have filter implementation yet")
	}

	switch o.Sort {
	case store.SortNatural:
		result, err := e.eclient.Search().Query(query).Index(e.elconfig.Index).Type(e.elconfig.IndexType).From(int(skip)).Size(int(o.Limit)).Do(context.Background())
		if err != nil {
			log.Errorf("Failed to get list hits in db %s", err.Error())
			return nil, err
		}
		if result.Hits.TotalHits.Value > 0 {
			for _, v := range result.Hits.Hits {
				serializable := factory.Factory()
				err := serializable.Unmarshal(v.Source)
				if err != nil {
					log.Errorf("Failed to unmarhsal data %s", err.Error())
				}
				list = append(list, serializable)
			}
		}

	case store.SortCreatedAsc:
		result, err := e.eclient.Search().Query(query).Index(e.elconfig.Index).Sort("CreatedAt", true).From(int(skip)).Size(int(o.Limit)).Do(context.Background())
		if err != nil {
			log.Errorf("Failed to get list hits in db %s", err.Error())
		}

		if result.Hits.TotalHits.Value > 0 {
			for _, v := range result.Hits.Hits {
				serializable := factory.Factory()
				err := serializable.Unmarshal(v.Source)
				if err != nil {
					log.Errorf("Failed to unmarhsal data %s", err.Error())
				}
				list = append(list, serializable)
			}
		}

	case store.SortCreatedDesc:
		result, err := e.eclient.Search().Query(query).Index(e.elconfig.Index).Sort("CreatedAt", false).Size(int(o.Limit)).From(int(skip)).Do(context.Background())
		if err != nil {
			log.Errorf("Failed to get list hits in db %s", err.Error())
		}
		if result.Hits.TotalHits.Value > 0 {
			for _, v := range result.Hits.Hits {
				serializable := factory.Factory()
				err := serializable.Unmarshal(v.Source)
				if err != nil {
					log.Errorf("Failed to unmarhsal data %s", err.Error())
				}
				list = append(list, serializable)
			}
		}

	case store.SortUpdatedAsc:
		result, err := e.eclient.Search().Query(query).Index(e.elconfig.Index).Sort("UpdatedAt", true).From(int(skip)).Size(int(o.Limit)).Do(context.Background())
		if err != nil {
			log.Errorf("Failed to get list hits in db %s", err.Error())
		}
		if result.Hits.TotalHits.Value > 0 {
			for _, v := range result.Hits.Hits {
				serializable := factory.Factory()
				err := serializable.Unmarshal(v.Source)
				if err != nil {
					log.Errorf("Failed to unmarhsal data %s", err.Error())
				}
				if o.Filter != nil {
					if !o.Filter.Compare(serializable) {
						continue
					}
				}
				list = append(list, serializable)
			}
		}

	case store.SortUpdatedDesc:
		result, err := e.eclient.Search().Query(query).Index(e.elconfig.Index).Sort("UpdatedAt", false).From(int(skip)).Size(int(o.Limit)).Do(context.Background())
		if err != nil {
			log.Errorf("Failed to get list hits in db %s", err.Error())
		}
		if result.Hits.TotalHits.Value > 0 {
			for _, v := range result.Hits.Hits {
				serializable := factory.Factory()
				err := serializable.Unmarshal(v.Source)
				if err != nil {
					log.Errorf("Failed to unmarhsal data %s", err.Error())
				}
				list = append(list, serializable)
			}
		}
	}
	return list, nil
}

func (e *SsElastic) Close() error {
	if e.eclient == nil {
		return nil
	}
	_, err := e.eclient.CloseIndex(e.elconfig.Index).Do(context.Background())
	if err != nil {
		log.Errorf("Unable to close %s :%s", e.elconfig.Index, err.Error())
		return nil
	}
	return nil
}
