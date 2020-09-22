package ss_elastic_store

import (
	"context"
	"errors"
	"fmt"
	store "github.com/StreamSpace/ss-store"
	"github.com/google/uuid"
	logger "github.com/ipfs/go-log/v2"
	elastic "github.com/olivere/elastic/v7"
)

var log = logger.Logger("store/elastic")

type ElasticStoreConfig struct {
	Url       string
	Index     string
	IndexType string
}

type ssElastic struct {
	eclient  *elastic.Client
	elconfig *ElasticStoreConfig
}

func (es *ElasticStoreConfig) Handler() string {
	return "elasticdb"
}

func NewElasticStore(config *ElasticStoreConfig) (*ssElastic, error) {
	eclient, err := elastic.NewClient(elastic.SetURL(config.Url))
	if err != nil {
		log.Errorf("Failed to create elastic client %s", err.Error())
		return nil, err
	}
	return &ssElastic{
		eclient:  eclient,
		elconfig: config,
	}, nil
}

func createID(i store.Item) string {
	return fmt.Sprintf("%s/%s", i.GetNamespace(), i.GetId())
}

func (e *ssElastic) Create(i store.Item) error {
	serializable, ok := i.(store.Serializable)
	if !ok {
		return errors.New("Unable to serialize")
	}
	resp, err := e.eclient.
		IndexExists(e.elconfig.Index).
		Do(context.Background())
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
	idSetter, ok := i.(store.IDSetter)
	if ok {
		idSetter.SetID(uuid.New().String())
	}
	data, err := serializable.Marshal()
	if err != nil {
		log.Errorf("Failed to serialize %s", err.Error())
		return err
	}
	_, err = e.eclient.Index().
		Index(e.elconfig.Index).
		Type(e.elconfig.IndexType).
		Id(createID(i)).
		BodyString(string(data)).
		Do(context.Background())
	if err != nil {
		log.Errorf("Faild to add document in index %s", err.Error())
		return err
	}
	return nil
}

func (e *ssElastic) Read(i store.Item) error {
	serializable, ok := i.(store.Serializable)
	if ok != true {
		return errors.New("item is not serializable")
	}
	resp, err := e.eclient.
		IndexExists(e.elconfig.Index).
		Do(context.Background())
	if err != nil {
		log.Debugf("Failed check index existence %s", err.Error())
		return err
	}

	if !resp {
		return errors.New("Index not created , read data failed")
	}

	getdata, err := e.eclient.Get().
		Index(e.elconfig.Index).
		Type(e.elconfig.IndexType).
		Id(createID(i)).
		Do(context.Background())
	if err != nil {
		log.Errorf("Failed to read data from db %s", err.Error())
		return err
	}
	err = serializable.Unmarshal(getdata.Source)
	if err != nil {
		log.Errorf("Failed to unmarshal received data %s", err.Error())
	}
	return nil
}

func (e *ssElastic) Delete(i store.Item) error {
	resp, err := e.eclient.
		IndexExists(e.elconfig.Index).
		Do(context.Background())
	if err != nil {
		log.Debugf("Failed check index existence %s", err.Error())
		return err
	}
	if !resp {
		return errors.New("Index not created , data deletion failed")
	}

	_, err = e.eclient.Delete().
		Index(e.elconfig.Index).
		Type(e.elconfig.IndexType).
		Id(createID(i)).
		Do(context.Background())
	if err != nil {
		log.Errorf("Document Deletion failed %s", err.Error())
		return err
	}
	return nil
}

func (e *ssElastic) Update(i store.Item) error {
	resp, err := e.eclient.
		IndexExists(e.elconfig.Index).
		Do(context.Background())
	if err != nil {
		log.Debugf("Failed check index existence %s", err.Error())
		return err
	}
	if !resp {
		return errors.New("Index doesn't exist , data updation is failed")
	}

	_, err = e.eclient.Update().
		Index(e.elconfig.Index).
		Type(e.elconfig.IndexType).
		Id(createID(i)).
		Doc(i).
		Do(context.Background())
	if err != nil {
		log.Errorf("Document updation is failed %s", err.Error())
		return err
	}
	return nil
}

func (e *ssElastic) List(factory store.Factory, o store.ListOpt) (store.Items, error) {
	var (
		list = []store.Item{}
		skip = o.Page * o.Limit
	)
	resp, err := e.eclient.
		IndexExists(e.elconfig.Index).
		Do(context.Background())
	if err != nil {
		log.Errorf("Failed to check index existence %s", err.Error())
		return nil, err
	}
	if !resp {
		return nil, errors.New("Index doesn't exist , list operation is failed")
	}
	listId := fmt.Sprintf("%s/", factory.Factory().GetNamespace())
	var query *elastic.BoolQuery
	query = elastic.NewBoolQuery().
		Must(elastic.NewSimpleQueryStringQuery(listId))
	if o.Filter != nil {
		return nil, errors.New("We don't have filter implementation yet")
	}

	if o.Sort != store.SortNatural {
		return nil, errors.New("We don't have sorting list")
	}
	result, err := e.eclient.Search().
		Query(query).
		Index(e.elconfig.Index).
		Type(e.elconfig.IndexType).
		From(int(skip)).
		Size(int(o.Limit)).
		Do(context.Background())
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
	return list, nil
}

func (e *ssElastic) Close() error {
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
