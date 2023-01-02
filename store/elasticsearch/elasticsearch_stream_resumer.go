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

var _ projection.ResumeStore = (*StreamResumer)(nil)

type GetResponse struct {
	ID      string      `json:"_id"`
	Version int64       `json:"_version"`
	Source  interface{} `json:"_source"`
}

type StreamResumerRow struct {
	ID    string `json:"id"`
	Token string `json:"token"`
}

type StreamResumer struct {
	client *elasticsearch.Client
	index  string
}

func NewStreamResumer(addresses []string, index string) (StreamResumer, error) {
	escfg := elasticsearch.Config{
		Addresses: addresses,
	}
	es, err := elasticsearch.NewClient(escfg)
	if err != nil {
		return StreamResumer{}, faults.Errorf("Error creating elastic search client: %w", err)
	}

	return StreamResumer{
		client: es,
		index:  index,
	}, nil
}

func (es StreamResumer) GetStreamResumeToken(ctx context.Context, key projection.ResumeKey) (string, error) {
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
		return "", faults.Wrap(projection.ErrResumeTokenNotFound)
	}

	if res.IsError() {
		return "", faults.Errorf("[%s] Error getting document ID=%s", res.Status(), key)
	}
	// Deserialize the response into a map.
	r := GetResponse{
		Source: &StreamResumerRow{},
	}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return "", faults.Errorf("Error parsing the response body for GetRequest: %w", err)
	}
	row := r.Source.(*StreamResumerRow)

	return row.Token, nil
}

func (es StreamResumer) SetStreamResumeToken(ctx context.Context, key projection.ResumeKey, token string) error {
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
