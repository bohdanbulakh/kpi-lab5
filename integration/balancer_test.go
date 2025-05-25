package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	requestCount := 20
	seenBackends := make(map[string]bool)

	for i := 0; i < requestCount; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		assert.NoError(t, err, "Request %d failed", i+1)
		assert.NotNil(t, resp, "Response should not be nil for request %d", i+1)

		if resp != nil {
			backend := resp.Header.Get("lb-from")
			assert.NotEmpty(t, backend, "Request %d missing lb-from header", i+1)
			t.Logf("Request %d served by [%s]", i+1, backend)
			seenBackends[backend] = true
			_ = resp.Body.Close()
		}
	}

	assert.GreaterOrEqual(t, len(seenBackends), 2, "Expected responses from at least 2 different backends")
}
