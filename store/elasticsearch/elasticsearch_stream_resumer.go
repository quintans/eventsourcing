package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/projection"
)

var _ projection.StreamResumer = (*ElasticSearchStreamResumer)(nil)

type ElasticGetResponse struct {
	ID      string      `json:"_id"`
	Version int64       `json:"_version"`
	Source  interface{} `json:"_source"`
}

type ElasticSearchStreamResumerRow struct {
	ID    string `json:"id"`
	Token string `json:"token"`
}

type ElasticSearchStreamResumer struct {
	client *elasticsearch.Client
	index  string
}

func NewElasticSearchStreamResumer(addresses []string, index string) (ElasticSearchStreamResumer, error) {
	escfg := elasticsearch.Config{
		Addresses: addresses,
	}
	es, err := elasticsearch.NewClient(escfg)
	if err != nil {
		return ElasticSearchStreamResumer{}, faults.Errorf("Error creating elastic search client: %w", err)
	}

	return ElasticSearchStreamResumer{
		client: es,
		index:  index,
	}, nil
}

func (es ElasticSearchStreamResumer) GetStreamResumeToken(ctx context.Context, key projection.StreamResume) (string, error) {
	req := esapi.GetRequest{
		Index:      es.index,
		DocumentID: key.String(),
	}
	res, err := req.Do(ctx, es.client)
	if err != nil {
		return "", faults.Errorf("Error getting response for GetRequest: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return "", nil
	}

	if res.IsError() {
		return "", faults.Errorf("[%s] Error getting document ID=%s", res.Status(), key)
	}
	// Deserialize the response into a map.
	r := ElasticGetResponse{
		Source: &ElasticSearchStreamResumerRow{},
	}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return "", faults.Errorf("Error parsing the response body for GetRequest: %w", err)
	}
	row := r.Source.(*ElasticSearchStreamResumerRow)

	return row.Token, nil
}

func (es ElasticSearchStreamResumer) SetStreamResumeToken(ctx context.Context, key projection.StreamResume, token string) error {
	res, err := es.client.Update(
		es.index,
		key.String(),
		strings.NewReader(fmt.Sprintf(`{
		  "doc": {
			"token": "%s"
		  },
		  "doc_as_upsert": true
		}`, token)),
	)
	if err != nil {
		return faults.Errorf("Error getting elastic search response: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return faults.Errorf("[%s] Error updating elastic search index '%s' %s=%s", res.Status(), es.index, key, token)
	}
	return nil
}
