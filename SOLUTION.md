# Solution Steps

1. 1. Define a Product struct and a simulated (thread-safe) in-memory database for products.

2. 2. Set up Redis connectivity and define helper functions for generating cache and hit count keys for each product.

3. 3. Implement the GET /product/{id} endpoint: Check Redis for cached product data. If present, serve from cache and increment a popularity counter; for popular items, refresh TTLs asynchronously. If not cached, fetch from DB and cache result with TTL and a hit count.

4. 4. Implement the PUT /product/{id} endpoint: Decode incoming JSON, update the simulated DB, and immediately invalidate related Redis cache keys (product data and hits).

5. 5. Start a background goroutine on startup to periodically scan Redis for cache keys matching 'product:*' and delete expired/stale keys (double-check in case Redis did not clean up).

6. 6. Ensure that cache hit rates are boosted (through hit counters and TTL extension for popular items), cache invalidation on updates prevents serving stale data, and expired/stale keys are cleaned up efficiently and safely.

7. 7. Organize the code in 'main.go' with dependencies tracked in 'go.mod'.

