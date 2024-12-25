package sunrise

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/rollkit/go-da"
)

type PublishRequest struct {
	Blob             string `json:"blob"`
	DataShardCount   int    `json:"data_shard_count"`
	ParityShardCount int    `json:"parity_shard_count"`
	Protocol         string `json:"protocol"`
}

type PublishResponse struct {
	TxHash      string `json:"tx_hash"`
	MetadataUri string `json:"metadata_uri"`
}

type GetBlobResponse struct {
	Blob string `json:"blob"`
}

type Config struct {
	ServerURL         string `json:"server_url"`
	DataShardCount    uint32 `json:"data_shard_count"`
	ParityShardCount  uint32 `json:"parity_shard_count"`
	GRPCServerAddress string `json:"grpc_server_address"`
}

type SunriseDA struct {
	ctx    context.Context
	config Config
}

func NewSunriseDA(ctx context.Context, config Config) *SunriseDA {
	return &SunriseDA{
		ctx:    ctx,
		config: config,
	}
}

var _ da.DA = &SunriseDA{}

func (sunrise *SunriseDA) MaxBlobSize(ctx context.Context) (uint64, error) {
	var maxBlobSize uint64 = 64 * 64 * 500
	return maxBlobSize, nil
}

func (sunrise *SunriseDA) Submit(ctx context.Context, daBlobs []da.Blob, gasPrice float64, namespace da.Namespace) ([]da.ID, error) {
	resultChan := make(chan PublishResponse, len(daBlobs))
	errorChan := make(chan error, len(daBlobs))

	var wg sync.WaitGroup

	var mu sync.Mutex

	for _, blob := range daBlobs {
		wg.Add(1)

		// Start a goroutine for each blob
		go func(blob da.Blob) {
			defer wg.Done()
			encodedBlob := base64.StdEncoding.EncodeToString(blob)
			requestData := PublishRequest{
				Blob:             encodedBlob,
				DataShardCount:   int(sunrise.config.DataShardCount),
				ParityShardCount: int(sunrise.config.ParityShardCount),
				Protocol:         "ipfs",
			}

			requestBody, err := json.Marshal(requestData)
			if err != nil {
				errorChan <- err
				return
			}

			response, err := http.Post(sunrise.config.ServerURL+"/publish", "application/json", bytes.NewBuffer(requestBody))
			if err != nil {
				errorChan <- err
				return
			}

			defer func() {
				err = response.Body.Close()
				if err != nil {
					log.Println("error closing response body", err)
				}
			}()

			responseData, err := io.ReadAll(response.Body)
			if err != nil {
				errorChan <- err
				return
			}

			var publishResponse PublishResponse
			err = json.Unmarshal(responseData, &publishResponse)
			if err != nil {
				errorChan <- err
				return
			}

			// Acquire the mutex before updating slices
			mu.Lock()
			resultChan <- PublishResponse{
				TxHash:      publishResponse.TxHash,
				MetadataUri: publishResponse.MetadataUri,
			}
			mu.Unlock()

		}(blob)
	}

	go func() {
		wg.Wait()
		close(resultChan)
		close(errorChan)
	}()

	// Collect results from channels
	var ids []da.ID

	for result := range resultChan {
		ids = append(ids, []byte(result.MetadataUri))
	}

	// Check for errors
	if err := <-errorChan; err != nil {
		return nil, err
	}
	return ids, nil
}

func (sunrise *SunriseDA) Get(ctx context.Context, ids []da.ID, namespace da.Namespace) ([]da.Blob, error) {
	var blobs [][]byte
	var metadataUri string
	for _, id := range ids {
		metadataUri = string(id)
		req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/get-blob?metadata_uri=%s", sunrise.config.ServerURL, metadataUri), nil)
		if err != nil {
			return nil, err
		}
		client := http.DefaultClient
		response, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer func() {
			err = response.Body.Close()
			if err != nil {
				log.Println("error closing response body", err)
			}
		}()
		responseData, err := io.ReadAll(response.Body)
		if err != nil {
			return nil, err
		}
		var blobResponse GetBlobResponse

		err = json.Unmarshal(responseData, &blobResponse)
		if err != nil {
			return nil, err
		}

		var blob = blobResponse.Blob

		decodedBlob, err := base64.StdEncoding.DecodeString(blob)
		if err != nil {
			return nil, err
		}

		blobs = append(blobs, decodedBlob)
	}
	return blobs, nil
}

func (c *SunriseDA) GetIDs(ctx context.Context, height uint64, namespace da.Namespace) ([]da.ID, error) {
	heightAsUint32 := uint32(height)
	ids := make([]byte, 8)
	binary.BigEndian.PutUint32(ids, heightAsUint32)

	return [][]byte{ids}, nil
}

func (sunrise *SunriseDA) GetProofs(ctx context.Context, ids []da.ID, namespace da.Namespace) ([]da.Proof, error) {
	var proofs []da.Proof

	return proofs, nil
}

func (sunrise *SunriseDA) Commit(ctx context.Context, daBlobs []da.Blob, namespace da.Namespace) ([]da.Commitment, error) {
	var commitments []da.Commitment

	return commitments, nil
}

func (sunrise *SunriseDA) Validate(ctx context.Context, ids []da.ID, daProofs []da.Proof, namespace da.Namespace) ([]bool, error) {
	var valid []bool

	return valid, nil
}
