package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Represents all the high level information about the problem
type Problem struct {
	numVideos         int
	numEndpoints      int
	numRequestBatches int
	numCaches         int
	cacheCapacity     int   // the max capacity of each cache server in mb
	videoSizes        []int // array where index are video IDs and values are video sizes in mb
	endpoints         []Endpoint
	caches            []CacheServer
	requestBatches    []RequestBatch
}

type Solution struct {
	assignments   []CacheAssignment
	vcPairs       []VideoCachePair
    videoRequests [][]int // maps videoID to list of requests for that video
}

type RequestBatch struct {
	videoID    int
	endpointID int
	amount     int
}

type Endpoint struct {
	id                int
	numCaches         int
	datacenterLatency int
	cacheLatencies    map[int]int     // maps cache server IDs to latencies
	preferredCaches   []IDLatencyPair // ordered list of cache ids based on latency (lowest first)
    requests          []int // list of requests on this endpoint
}

type CacheServer struct {
	id                int
	endpointLatencies map[int]int
}

type CacheAssignment struct {
	cacheID      int
	usedCapacity int
	videos       map[int]bool // set of assigned videos
}

type ParseState struct {
	lineNumber           int
	currentEndpoint      *Endpoint
	endpointParseCounter int
	nextEndpointID       int
	nextRequestID        int
}

// Useful for a number of purposes, this is a tuple containing the id of some object and it's latency to some
// other (unspecified) object
type IDLatencyPair struct {
	id      int
	latency int
}

// A pairing of a video with a cache server along with the saving that incurs
type VideoCachePair struct {
	videoID          int
	cacheID          int
    affectedRequests []int
	saving           int
    uses             int // how many requests actually use this cache?
    assigned         bool // whether it's in use or not
    ignoreSet        map[int]bool // list of request IDs to ignore (they aren't helped by the pair)
}

func initVideos(p *Problem) {
	p.videoSizes = make([]int, p.numVideos, p.numVideos)
}

// Create an appropriate number of CacheServer structs and add them to the problem
func initCaches(p *Problem) {
	p.caches = make([]CacheServer, p.numCaches, p.numCaches)
	for i := 0; i < p.numCaches; i++ {
		p.caches[i] = CacheServer{id: i, endpointLatencies: make(map[int]int)}
	}
}

// Create an appropriate number of Endpoint structs and add them to the problem
func initEndpoints(p *Problem) {
	p.endpoints = make([]Endpoint, p.numEndpoints, p.numEndpoints)
	for i := 0; i < p.numEndpoints; i++ {
		p.endpoints[i] = Endpoint{id: i, cacheLatencies: make(map[int]int)}
	}
}

// Create an appropriate number of RequestBatches structs and add them to the problem
func initRequestBatches(p *Problem) {
	p.requestBatches = make([]RequestBatch, p.numRequestBatches, p.numRequestBatches)
	for i := 0; i < p.numRequestBatches; i++ {
		p.requestBatches[i] = RequestBatch{}
	}
}

// Arguments are a pointer to an int field in a struct
// And a string which is believed to be an integer like "5"
// If parse successful then set the struct field, else return error.
func stringToIntHelper(ptr *int, s string) error {
	i, err := strconv.Atoi(s)
	if err != nil {
		panic("Couldn't parse int from string " + s)
	} else {
		//fmt.Printf("Parsed int %d from string %s\n", i, s)
		*ptr = i
	}

	return nil
}

func parseInputFile(filePath string) (Problem, error) {
	var problem Problem

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Can't open file for reading")
		log.Fatal(err)
		return problem, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	state := ParseState{}

	for scanner.Scan() {
		line := scanner.Text()
		//fmt.Printf("%d: %s. %+v\n", state.lineNumber, line, state)
		state.lineNumber++

		switch state.lineNumber {
		case 1:
			//fmt.Println("Parsing first line")
			parseFirstLine(&problem, line)
			initVideos(&problem)
			initCaches(&problem)
			initEndpoints(&problem)
			initRequestBatches(&problem)
		case 2:
			//fmt.Println("Parsing second line")
			parseSecondLine(&problem, line)
		default:
			//fmt.Println("Parsing late line")
			words := strings.Fields(line)
			switch len(words) {
			case 2:
				parseEndpointLine(&problem, &state, line)
			case 3:
				//fmt.Println("Parsing request line")
				parseRequestLine(&problem, &state, line)
			default:
				return problem, errors.New("Failed cause too many words on line " + string(state.lineNumber))
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal("Scanner error")
		log.Fatal(err)
		return problem, err
	}

	return problem, nil
}

// Helper function to parse the first line of the problem file
func parseFirstLine(p *Problem, line string) error {
	words := strings.Fields(line)

	for i, word := range words {
		//fmt.Printf("Word %d: %s\n", i, word)

		switch i {
		case 0:
			stringToIntHelper(&p.numVideos, word)
		case 1:
			stringToIntHelper(&p.numEndpoints, word)
		case 2:
			stringToIntHelper(&p.numRequestBatches, word)
		case 3:
			stringToIntHelper(&p.numCaches, word)
		case 4:
			stringToIntHelper(&p.cacheCapacity, word)
		default:
			return errors.New("Parsing first line failed")
		}
	}

	return nil
}

func parseSecondLine(p *Problem, line string) error {
	words := strings.Fields(line)
	p.videoSizes = make([]int, p.numVideos, p.numVideos)

	for i, word := range words {
		stringToIntHelper(&p.videoSizes[i], word)
	}

	return nil
}

func parseEndpointLine(p *Problem, state *ParseState, line string) error {
	words := strings.Fields(line)

	if state.endpointParseCounter == 0 {
		state.currentEndpoint = &p.endpoints[state.nextEndpointID]
		state.nextEndpointID++
		stringToIntHelper(&state.currentEndpoint.datacenterLatency, words[0])
		stringToIntHelper(&state.currentEndpoint.numCaches, words[1]) // the number of cache servers attached
		stringToIntHelper(&state.endpointParseCounter, words[1])      // the number of cache servers attached
	} else {
		var cacheID int
		var cacheLatency int
		stringToIntHelper(&cacheID, words[0])
		stringToIntHelper(&cacheLatency, words[1])

		cache := p.caches[cacheID]
		state.currentEndpoint.cacheLatencies[cacheID] = cacheLatency
		cache.endpointLatencies[state.currentEndpoint.id] = cacheLatency
		state.endpointParseCounter--
	}
	return nil
}

func parseRequestLine(p *Problem, state *ParseState, line string) error {
	words := strings.Fields(line)

	req := &p.requestBatches[state.nextRequestID]
	stringToIntHelper(&req.videoID, words[0])
	stringToIntHelper(&req.endpointID, words[1])
	stringToIntHelper(&req.amount, words[2])

	state.nextRequestID++
	return nil
}

func calcRequestHeuristic(p *Problem, req *RequestBatch) float64 {
	sizeFactor := 1.0 - float64(p.videoSizes[req.videoID])/float64(p.cacheCapacity)
	endpoint := &p.endpoints[req.endpointID]

	var savingFactor float64
	if len(endpoint.preferredCaches) > 0 {
		savingFactor = 1.0 - float64(endpoint.preferredCaches[0].latency)/float64(endpoint.datacenterLatency)
	} else {
		savingFactor = 0.0
	}

	return float64(req.amount) * sizeFactor * savingFactor
}

func sortProblemRequests(p *Problem) {
	requestComparison := func(i, j int) bool {
		r1 := &p.requestBatches[i]
		r2 := &p.requestBatches[j]
		return calcRequestHeuristic(p, r1) > calcRequestHeuristic(p, r2)
	}
	sort.SliceStable(p.requestBatches, requestComparison)
}

// Sets the preferredCaches attribute for every endopint in the problem
// This is an ordered slice of cache ids based on latency
func setPreferredCaches(p *Problem) {
	for i := 0; i < p.numEndpoints; i++ {
		endpoint := &p.endpoints[i]
		endpoint.preferredCaches = make([]IDLatencyPair, endpoint.numCaches, endpoint.numCaches)

		compareIDLatencyPair := func(i, j int) bool {
			return endpoint.preferredCaches[i].latency < endpoint.preferredCaches[j].latency
		}

		// First add all the caches to preferredCaches, then sort
		ptr := 0
		for cacheID, latency := range endpoint.cacheLatencies {
			endpoint.preferredCaches[ptr] = IDLatencyPair{id: cacheID, latency: latency}
			ptr++
		}

		sort.SliceStable(endpoint.preferredCaches, compareIDLatencyPair)
	}
}

// Sets the requests field of every Endpoint
func setEndpointRequests(p *Problem) {
    fmt.Println("Set endpoint requests called")
    for reqID, req := range p.requestBatches {
        ep := &p.endpoints[req.endpointID]
        ep.requests = append(ep.requests, reqID)
    }

    /*
    for epID, ep := range p.endpoints {
        fmt.Printf("Endpoint %v has %v requests\n", epID, len(ep.requests))
    }
    */
}

// Set the videoRequests field of the solution
func setVideoRequests(p *Problem, sol *Solution) {
    sol.videoRequests = make([][]int, p.numVideos)

    for reqID, req := range p.requestBatches {
        sol.videoRequests[req.videoID] = append(sol.videoRequests[req.videoID], reqID)
    }

    for vid := 0; vid < p.numVideos; vid++ {
        numReqs := len(sol.videoRequests[vid])
        if numReqs > 0 {
            fmt.Printf("Video %v has %v requests\n", vid, numReqs)
        }
    }
}

func initSolutionAssignments(p *Problem, sol *Solution) {
	sol.assignments = make([]CacheAssignment, p.numCaches, p.numCaches)
	for i := 0; i < p.numCaches; i++ {
		sol.assignments[i] = CacheAssignment{cacheID: i, usedCapacity: 0, videos: make(map[int]bool)}
	}
}

func initSolutionVCPairs(p *Problem, sol *Solution) {
    // Should be called after setEndpointRequests
    fmt.Println("Initializing video-cache pairs")
    maxRequests := 0
    minRequests := 9999999
    totalNumReqs := 0

    // Maps videoID -> (cacheID -> *VideoCachePair)
    var usedPairs map[int]map[int]*VideoCachePair = make(map[int]map[int]*VideoCachePair)

    for vid := 0; vid < p.numVideos; vid++ {
        var vidRequests []int
        for reqID, req := range p.requestBatches {
            //fmt.Printf("Req %v %+v\n", reqID, req)
            if req.videoID == vid {
                vidRequests = append(vidRequests, reqID)
            }
        }

        numReqs := len(vidRequests)
        totalNumReqs += numReqs
        //fmt.Printf("Vid %v has %v requests\n", vid, numReqs)

        if numReqs < minRequests {
            minRequests = numReqs
        } else if numReqs > maxRequests {
            maxRequests = numReqs
        }

        for _, reqID := range vidRequests {
            req := p.requestBatches[reqID]
            ep := p.endpoints[req.endpointID]
            for _, idLatPair := range ep.preferredCaches {
                cID := idLatPair.id

                // Check if we have a pair for this video/cache already
                seen := false
                if usedPairs[vid] == nil {
                    usedPairs[vid] = make(map[int]*VideoCachePair)
                } else if usedPairs[vid][cID] != nil {
                    seen = true
                    pair := usedPairs[vid][cID]
                    pair.affectedRequests = append(pair.affectedRequests, reqID)
                }

                if !seen {
                    // Make a new pair
                    sol.vcPairs = append(sol.vcPairs, VideoCachePair{videoID: vid, cacheID: cID})
                    currentPair := &sol.vcPairs[len(sol.vcPairs)-1] // the newly added pair
                    currentPair.affectedRequests = append(currentPair.affectedRequests, reqID)
                    //currentPair.ignoreSet = make(map[int]bool) // SET THIS IF ACTUALLY NEEDED
                    usedPairs[vid][cID] = currentPair
                }
            }
        }
    }

    fmt.Printf("minRequests %v maxRequests %v numPairs %v totalNumReqs %v\n",
                minRequests, maxRequests, len(sol.vcPairs), totalNumReqs)
}

// Calls setVCPairSaving for every pair in the solution
func setVCPairSavings(p *Problem, sol *Solution) {
    fmt.Println("Setting savings...")
    for i := 0; i < len(sol.vcPairs); i++ {
        pair := &sol.vcPairs[i]
        if !pair.assigned {
            setVCPairSaving(p, sol, &sol.vcPairs[i])
        }
    }
}

// Calculates the potential saving if this pair is used (assuming that every request actually uses this cache)
// Pays respect to the ignoreSet of the pair (ignores those requests)
func setVCPairSaving(p *Problem, sol *Solution, pair *VideoCachePair) {
    //fmt.Printf("Setting saving for pair %+v\n", pair)
    cache := p.caches[pair.cacheID]

    totalBasicTime := 0
    totalCachedTime := 0
    for _, reqID := range pair.affectedRequests {
        req := p.requestBatches[reqID]
        //fmt.Printf("    Affects request %v\n", req)
        if pair.ignoreSet[reqID] {
            //fmt.Printf("    Ignoring this request\n")
            continue
        }

        ep := p.endpoints[req.endpointID]
        basicTime := req.amount * ep.datacenterLatency
        totalBasicTime += basicTime

        cachedTime := req.amount * cache.endpointLatencies[req.endpointID]
        totalCachedTime += cachedTime
    }

    pair.saving = totalBasicTime - totalCachedTime
    //fmt.Printf("Pair (vid %v, cache %v) saves %+v\n", pair.videoID, pair.cacheID, pair.saving)
}

func sortSolutionVCPairs(p *Problem, sol *Solution) {
    // Sorts all the pairs by the saving
    fmt.Println("Sorting pairs...")
    sort.SliceStable(sol.vcPairs, func(i int, j int) bool {
        return sol.vcPairs[i].saving > sol.vcPairs[j].saving
    })
}

func writeSolutionFile(filePath string, sol *Solution) {
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	f.WriteString(fmt.Sprintf("%d\n", len(sol.assignments)))

	for _, ass := range sol.assignments {
		//fmt.Printf("Iteration %d\n", i)
		//if len(ass.videos) == 0 {
		//	continue
		//}

		var line []byte
		line = append(line, []byte(fmt.Sprintf("%d ", ass.cacheID))...)

		for vid, _ := range ass.videos {
			line = append(line, []byte(fmt.Sprintf("%d ", vid))...)
		}

		line = append(line, '\n')

		f.WriteString(string(line))
	}
}

// Checks if the video in the request is already in a cache server attached to it's endpoint
func isAlreadyCached(p *Problem, req *RequestBatch, sol *Solution) (bool, int) {
	var endpoint *Endpoint = &p.endpoints[req.endpointID]
	//fmt.Printf("isAlreadyCached %+v\n", req)

	for _, idLatPair := range endpoint.preferredCaches {
		var ass *CacheAssignment = &sol.assignments[idLatPair.id]
		if ass.videos[req.videoID] {
			//fmt.Println("isAlreadyCached returning true")
			return true, idLatPair.latency
		}
	}

	//fmt.Println("isAlreadyCached returning false")
	return false, 0
}

func bestCacheLatency(p *Problem, ep *Endpoint) int {
	if len(ep.preferredCaches) > 0 {
		return ep.preferredCaches[0].latency
	}

	return ep.datacenterLatency
}

func getCacheUsedByRequest(p *Problem, sol *Solution, req *RequestBatch) int {
    ep := p.endpoints[req.endpointID]

    for _, idLatPair := range ep.preferredCaches {
        ass := sol.assignments[idLatPair.id]
        if ass.videos[req.videoID] {
            return idLatPair.id
        }
    }

    return -1
}

func computeSolutionScore(p *Problem, sol *Solution) int {
    score := 0
    totalRequestAmounts := 0

    for _, req := range p.requestBatches {
        ep := p.endpoints[req.endpointID]
        totalRequestAmounts += req.amount

        reqCacheID := getCacheUsedByRequest(p, sol, &req)
        reqLatency := ep.datacenterLatency

        // It has a cache
        if reqCacheID != -1 {
            reqLatency = ep.cacheLatencies[reqCacheID]
        }

        savedTime := (ep.datacenterLatency - reqLatency) * req.amount
        //fmt.Printf("Req %+v uses cache %v saved time %v\n", req, reqCacheID, savedTime)
        score += savedTime
    }

    return int(1000.0 * float64(score) / float64(totalRequestAmounts))
}

func printSolution(sol *Solution) {
    fmt.Println("Solution is:")
    /*
    for _, ass := range sol.assignments {
        fmt.Printf("%+v\n", ass)
    }
    */
}

func doAssignments(p *Problem, sol *Solution) {
    fmt.Println("Doing assignments")
    for cID := 0; cID < p.numCaches; cID++ {
        ass := sol.assignments[cID]
        if ass.usedCapacity == p.cacheCapacity {
            // No point trying cause it's already full
            //fmt.Println("Cache %v is already full so skipping")
            continue
        }
        freeSpace := p.cacheCapacity - ass.usedCapacity

        // Find all pairs for this cache that aren't already assigned
        // Some may have been unassigned during waste detection phase
        var pairsForCache []*VideoCachePair

        for i:=0; i < len(sol.vcPairs); i++ {
            pair := &sol.vcPairs[i]

            if pair.cacheID == cID && !pair.assigned {
                pairsForCache = append(pairsForCache, pair)
            }
        }

        // Parameters for bin packing problem
        var weights []int
        var values []int
        var n int = len(pairsForCache)-1
        var W int = freeSpace
        /*
        fmt.Printf("Computing knapsack solution for cache %v (%v items) (%v space)\n",
                    cID, len(pairsForCache), W)
                    */

        for _, pair := range pairsForCache {
            weights = append(weights, p.videoSizes[pair.videoID])
            values = append(values, pair.saving)
        }

        var resultDynamic [][]int
        resultDynamic = knapsackDynamic(weights, values, n, W)
        //var finalValue int
        var result []int
        _, result = interpretKnapsackSolution(values, weights, resultDynamic, W, n)

        //fmt.Printf("Solution found! %v pairs, finalValue %v\n", len(result), finalValue)
        for _, pairIndex := range result {
            pair := pairsForCache[pairIndex]
            // Use the pair
            sol.assignments[pair.cacheID].videos[pair.videoID] = true
            pair.assigned = true
        }

        /*
        if cID == 0 {
            fmt.Println("Debugging bin packing")
            fmt.Printf("Pairs %v\n", pairsForCache)
            fmt.Printf("Weights %v\n", weights)
            fmt.Printf("Values %v\n", values)
            fmt.Printf("Result %v\n", result)
        }
        */
    }
}

// Works out which pairs in the solution are not actually being used by requests
func calculateWaste(p *Problem, sol *Solution) {
    // Calculate wasteful assignments (not actually used by request)
    fmt.Println("Calculating waste")
    usefulPairs := 0
    wastefulPairs := 0
    var wastefulSet []int // list of indexes of the pairs that are wasteful
    for i:=0; i < len(sol.vcPairs); i++ {
        pair := &sol.vcPairs[i]
        pair.uses = 0

        // Count every use
        for _, reqID := range pair.affectedRequests {
            req := &p.requestBatches[reqID]
            cacheUsed := getCacheUsedByRequest(p, sol, req)

            if cacheUsed == pair.cacheID {
                pair.uses++
            } else if cacheUsed != -1 {
				// Memory optimization
				if pair.ignoreSet == nil {
					pair.ignoreSet = make(map[int]bool)
				}
                pair.ignoreSet[reqID] = true
            }
        }

        if pair.assigned {
            if pair.uses == 0 {
                //fmt.Printf("Pair is wasteful %+v\n", pair)
                wastefulPairs++
                wastefulSet = append(wastefulSet, i)
            } else {
                //fmt.Printf("Pair is useful %+v\n", pair)
                usefulPairs++
            }
        }
    }

    // Remove wasteful pairs from the solution
    for _, pairIndex := range wastefulSet {
        pair := &sol.vcPairs[pairIndex]
        pair.assigned = false

        ass := sol.assignments[pair.cacheID]
        ass.videos[pair.videoID] = false
        videoSize := p.videoSizes[pair.videoID]
        ass.usedCapacity -= videoSize
    }

    fmt.Printf("Useful pairs %d wasteful pairs %d\n", usefulPairs, wastefulPairs)
}

func solve(p *Problem) (sol Solution) {
	fmt.Println("Beginning to solve...")

	setPreferredCaches(p)
    setEndpointRequests(p)
    //setVideoRequests(p, &sol)
	//sortProblemRequests(p)
	initSolutionAssignments(p, &sol)
    initSolutionVCPairs(p, &sol)

    // Main loop
    iterations := 5
    for i:=0; i < iterations; i++ {
        fmt.Printf("ITERATION %d\n", i)
        setVCPairSavings(p, &sol)
        sortSolutionVCPairs(p, &sol)
        doAssignments(p, &sol)
        calculateWaste(p, &sol)
    }

    /*
    for _, pair := range sol.vcPairs {
        videoSize := p.videoSizes[pair.videoID]
        ass := &sol.assignments[pair.cacheID]

		if ass.usedCapacity+videoSize <= p.cacheCapacity {
			ass.videos[pair.videoID] = true
			ass.usedCapacity += videoSize
        }
    }
    */

    /*
	for _, req := range p.requestBatches {
		var videoSize int = p.videoSizes[req.videoID]
		var endpoint *Endpoint = &p.endpoints[req.endpointID]

		if bestCacheLatency(p, endpoint) >= endpoint.datacenterLatency {
			// No point caching
			continue
		}

		// Video is already present in a cache server attached to the endpoint
		alreadyCached, _ := isAlreadyCached(p, &req, &sol)
		if alreadyCached {
			continue
		}

		for _, idLatPair := range endpoint.preferredCaches {
			// If there's a substantial saving from a better cache then use it
			var ass *CacheAssignment = &sol.assignments[idLatPair.id]
			if ass.usedCapacity+videoSize <= p.cacheCapacity {
				//fmt.Printf("Cached in %v\n", ass.cacheID)
				// Add the video to the cache
				ass.videos[req.videoID] = true
				ass.usedCapacity += videoSize
				break
			}
		}
	}
    */

    printSolution(&sol)
	fmt.Printf("Finished! Score %v\n", computeSolutionScore(p, &sol))
	return sol
}

func main() {
	if len(os.Args) != 2 {
		fmt.Print("Usage: hashgode <path-to-input>")
		return
	}

	inputPath := os.Args[1]
	problem, err := parseInputFile(inputPath)
	if err != nil {
		log.Fatal("parseInputFile failed")
		log.Fatal(err)
		return
	}
	fmt.Printf("Input file is %v\n", inputPath)

	solution := solve(&problem)
	writeSolutionFile(inputPath+".solution", &solution)
}
