package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const baseAddress = "http://balancer:8090"
const teamKey = "dreamteam"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	time.Sleep(5 * time.Second)

	requestCount := 10
	seenBackends := make(map[string]bool)
	expectedValue := ""

	for i := 0; i < requestCount; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=%s", baseAddress, teamKey))
		assert.NoError(t, err, "Request %d failed", i+1)
		assert.Equal(t, http.StatusOK, resp.StatusCode, "Unexpected status on request %d", i+1)

		if resp != nil {
			backend := resp.Header.Get("lb-from")
			assert.NotEmpty(t, backend, "Request %d missing lb-from header", i+1)
			seenBackends[backend] = true

			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			var result map[string]string
			_ = json.Unmarshal(body, &result)
			val := result["value"]

			assert.NotEmpty(t, val, "Expected non-empty value from DB")
			if i == 0 {
				expectedValue = val
			} else {
				assert.Equal(t, expectedValue, val, "Expected consistent value from DB")
			}
		}
	}

	assert.GreaterOrEqual(t, len(seenBackends), 2, "Expected responses from at least 2 different backends")
}
