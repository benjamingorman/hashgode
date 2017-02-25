package main

import (
	"testing"
)

func TestStringToIntHelper(t *testing.T) {
	var x int = 0
	stringToIntHelper(&x, "42")
	if x != 42 {
		t.Fail()
	}
}

func TestParseInputFile(t *testing.T) {
	inputFile := "data/test1.in"
	p, err := parseInputFile(inputFile)

	if err != nil {
		t.Fail()
	}

	// First line
	if p.numVideos != 4 {
		t.Fail()
	}
	if p.numEndpoints != 2 {
		t.Fail()
	}
	if p.numRequestBatches != 6 {
		t.Fail()
	}
	if p.numCaches != 3 {
		t.Fail()
	}
	if p.cacheCapacity != 120 {
		t.Fail()
	}

	// Second line
	if p.videoSizes[0] != 30 {
		t.Fail()
	}
	if p.videoSizes[1] != 50 {
		t.Fail()
	}
	if p.videoSizes[2] != 70 {
		t.Fail()
	}
	if p.videoSizes[3] != 90 {
		t.Fail()
	}

	// Rest
	if p.endpoints[0].datacenterLatency != 1000 {
		t.Errorf("datacenterLatency want %d got %d\n", 1000, p.endpoints[0].datacenterLatency)
		t.Fail()
	}
	if p.endpoints[0].numCaches != 3 {
		t.Errorf("numCaches want %d got %d\n", 2, p.endpoints[0].numCaches)
		t.Fail()
	}
	if p.endpoints[0].cacheLatencies[0] != 100 {
		t.Errorf("cacheLatencies[0] want %d got %d\n", 100, p.endpoints[0].cacheLatencies[0])
		t.Fail()
	}
	if p.endpoints[0].cacheLatencies[1] != 200 {
		t.Errorf("cacheLatencies[1] want %d got %d\n", 200, p.endpoints[1].cacheLatencies[0])
		t.Fail()
	}

	// Caches
	if p.caches[0].endpointLatencies[0] != 100 {
		t.Errorf("endpointLatencies[0] want %d got %d\n", 100, p.caches[0].endpointLatencies[0])
		t.Fail()
	}
	if p.caches[1].endpointLatencies[0] != 200 {
		t.Errorf("endpointLatencies[0] want %d got %d\n", 200, p.caches[1].endpointLatencies[0])
		t.Fail()
	}

	// Request batches
	if p.requestBatches[1].videoID != 1 {
		t.Errorf("requestBatches[1].videoID want %d got %d\n", 1, p.requestBatches[1].videoID)
		t.Fail()
	}
	if p.requestBatches[1].endpointID != 0 {
		t.Errorf("requestBatches[1].endpointID want %d got %d\n", 0, p.requestBatches[1].endpointID)
		t.Fail()
	}
	if p.requestBatches[1].amount != 500 {
		t.Errorf("requestBatches[1].amount want %d got %d\n", 500, p.requestBatches[1].amount)
		t.Fail()
	}
}

func testHelper(want *int, got *int, t *testing.T) func(string) {
	return func(msg string) {
		if *got != *want {
			t.Errorf("FAILED %s. want %v got %v", msg, *want, *got)
			t.Fail()
		}
	}
}

func TestCalcRequestHeuristic(t *testing.T) {
	p, _ := parseInputFile("data/test1.in")
	req := &p.requestBatches[0]
	setPreferredCaches(&p)

	var want float64 = float64(0.9 * 1000.0 * (1.0 - 30.0/120.0))
	var got float64 = calcRequestHeuristic(&p, req)

	if want != got {
		t.Errorf("Failed heuristic test, want %v got %v", want, got)
		t.Fail()
	}
}

func TestSortProblemRequests(t *testing.T) {
	t.Skip()
	p, _ := parseInputFile("data/test1.in")
	setPreferredCaches(&p)
	sortProblemRequests(&p)

	var want int
	var got int
	test := testHelper(&want, &got, t)

	want, got = 1000, p.requestBatches[0].amount
	test("0 amount correct")

	want, got = 500, p.requestBatches[1].amount
	test("1 amount correct")
}

func TestSetPreferredCaches(t *testing.T) {
	p, _ := parseInputFile("data/test1.in")
	setPreferredCaches(&p)

	var want int
	var got int
	test := testHelper(&want, &got, t)

	ep := p.endpoints[0]
	want, got = 0, ep.preferredCaches[0].id
	test("preferredCaches[0].id")

	want, got = 100, ep.preferredCaches[0].latency
	test("preferredCaches[0].latency")

	want, got = 2, ep.preferredCaches[1].id
	test("preferredCaches[1].id")

	want, got = 150, ep.preferredCaches[1].latency
	test("preferredCaches[1].latency")
}
