package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChooseServerWithLeastTraffic_ChoosesLeastTraffic(t *testing.T) {
	healthMutex.Lock()
	healthyServers = []string{"server1:8080", "server2:8080", "server3:8080"}
	healthMutex.Unlock()

	statsMutex.Lock()
	trafficStats["server1:8080"] = 1000
	trafficStats["server2:8080"] = 500
	trafficStats["server3:8080"] = 900
	statsMutex.Unlock()

	server, err := chooseServerWithLeastTraffic()
	assert.NoError(t, err)
	assert.Equal(t, "server2:8080", server)
}

func TestChooseServerWithLeastTraffic_EmptyHealthy(t *testing.T) {
	healthMutex.Lock()
	healthyServers = []string{}
	healthMutex.Unlock()

	server, err := chooseServerWithLeastTraffic()
	assert.Error(t, err)
	assert.Empty(t, server)
}

func TestChooseServerWithLeastTraffic_EqualTraffic(t *testing.T) {
	healthMutex.Lock()
	healthyServers = []string{"server1:8080", "server2:8080"}
	healthMutex.Unlock()

	statsMutex.Lock()
	trafficStats["server1:8080"] = 1000
	trafficStats["server2:8080"] = 1000
	statsMutex.Unlock()

	server, err := chooseServerWithLeastTraffic()
	assert.NoError(t, err)
	assert.Contains(t, []string{"server1:8080", "server2:8080"}, server)
}

func TestChooseServerWithLeastTraffic_OneHealthy(t *testing.T) {
	healthMutex.Lock()
	healthyServers = []string{"server3:8080"}
	healthMutex.Unlock()

	statsMutex.Lock()
	trafficStats["server3:8080"] = 777
	statsMutex.Unlock()

	server, err := chooseServerWithLeastTraffic()
	assert.NoError(t, err)
	assert.Equal(t, "server3:8080", server)
}
