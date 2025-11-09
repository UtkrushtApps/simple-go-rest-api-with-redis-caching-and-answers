package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
)

// Product represents a product entity
// For demo purposes, we implement only a few fields
// In practice, you might want to expand this struct
//
type Product struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Price int    `json:"price"`
}

// Simulated DB
var (
	fakeProductDB = map[int]*Product{
		1: {ID: 1, Name: "Apple", Price: 100},
		2: {ID: 2, Name: "Banana", Price: 50},
		3: {ID: 3, Name: "Cherry", Price: 200},
	}
	fakeDBLock = &sync.RWMutex{}
)

const (
	redisProductKeyPrefix = "product:"
	redisProductTTL       = 30 * time.Second // e.g., 30s TTL
	popularThreshold      = 2                // min hits to refresh TTL
)

var (
	redisClient *redis.Client
	bgWg        sync.WaitGroup
)

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	redisClient = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}

	// Start the cache cleaner background goroutine
	bgWg.Add(1)
	go func() {
		defer bgWg.Done()
		runCacheCleaner(ctx)
	}()

	r := mux.NewRouter()
	r.HandleFunc("/product/{id:[0-9]+}", getProductHandler).Methods("GET")
	r.HandleFunc("/product/{id:[0-9]+}", updateProductHandler).Methods("PUT")

	log.Println("Listening on :8080...")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatalf("HTTP server error: %v", err)
	}
}

// Utility - build Redis key for a product
func redisProductKey(id int) string {
	return fmt.Sprintf("%s%d", redisProductKeyPrefix, id)
}

// Utility - build Redis hit count key for a product
func redisProductHitsKey(id int) string {
	return fmt.Sprintf("%s%d:hits", redisProductKeyPrefix, id)
}

// Handler - GET /product/{id}
func getProductHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	idStr := vars["id"]
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid product id", http.StatusBadRequest)
		return
	}

	redisKey := redisProductKey(id)
	redisHitsKey := redisProductHitsKey(id)
	var product Product

	cacheHit := false
	data, err := redisClient.Get(ctx, redisKey).Result()
	if err == nil {
		if err := json.Unmarshal([]byte(data), &product); err == nil {
			cacheHit = true
			// Increment hit count
			hits, _ := redisClient.Incr(ctx, redisHitsKey).Result()

			if hits >= popularThreshold {
				// Refresh TTL for popular items
				redisClient.Expire(ctx, redisKey, redisProductTTL)
				redisClient.Expire(ctx, redisHitsKey, redisProductTTL)
			}
		}
	}
	if !cacheHit {
		// Not found or not deserialized; get from DB
		fakeDBLock.RLock()
		dbProduct, ok := fakeProductDB[id]
		fakeDBLock.RUnlock()
		if !ok {
			http.Error(w, "Product not found", http.StatusNotFound)
			return
		}
		product = *dbProduct

		raw, _ := json.Marshal(product)
		redisClient.Set(ctx, redisKey, raw, redisProductTTL)
		redisClient.Set(ctx, redisHitsKey, 1, redisProductTTL)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(product)
}

// Handler - PUT /product/{id}
func updateProductHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	idStr := vars["id"]
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid product id", http.StatusBadRequest)
		return
	}

	var input Product
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if input.ID != id {
		http.Error(w, "ID in path and body mismatch", http.StatusBadRequest)
		return
	}

	// Update fake DB
	fakeDBLock.Lock()
	fakeProductDB[id] = &Product{ID: id, Name: input.Name, Price: input.Price}
	fakeDBLock.Unlock()

	// Invalidate related cache keys immediately after update
	redisKey := redisProductKey(id)
	redisHitsKey := redisProductHitsKey(id)
	redisClient.Del(ctx, redisKey)
	redisClient.Del(ctx, redisHitsKey)

	w.WriteHeader(http.StatusNoContent)
}

// Background goroutine - clean expired keys
func runCacheCleaner(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cleanStaleProductKeys(ctx)
		}
	}
}

// Remove keys in background that are already expired or stale (belt and suspenders)
func cleanStaleProductKeys(ctx context.Context) {
	// Efficiently scan keys with pattern product:*
	var (
		cursor uint64 = 0
		scanCount      = int64(100)
	)
	for {
		// Scan for keys
		keys, nextCursor, err := redisClient.Scan(ctx, cursor, redisProductKeyPrefix+"*", scanCount).Result()
		if err != nil {
			log.Printf("Cache cleaner scan error: %v", err)
			return
		}
		for _, key := range keys {
			// For each key, check TTL. If expired, remove.
			ttl, err := redisClient.TTL(ctx, key).Result()
			if err == nil && (ttl <= 0 || ttl == -1) {
				redisClient.Del(ctx, key)
			}
		}
		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}
}