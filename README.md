# Singapore Resale Price Analysis - Performance Comparison

This project implements and compares different data storage and query optimization approaches for analyzing Singapore's resale price data. We explore various techniques to optimize the performance of filtering operations on the dataset.

## Approaches Implemented

### 1. Original Approach (Column Store)
A basic columnar storage implementation that stores each column separately in memory.

**Pros:**
- Simple implementation
- Direct access to column data
- Good for column-oriented operations
- Low memory overhead

**Cons:**
- Sequential scan required for filtering
- No optimization for multi-column queries
- No data pruning capabilities

### 2. File-Based Column Store
An extension of the column store that persists columns to disk and implements sequential filtering.

**Pros:**
- Reduced memory usage through file-based storage
- Sequential filtering optimization
- Persistent storage
- Good for large datasets that don't fit in memory

**Cons:**
- I/O overhead for column access
- Still requires sequential scan
- Disk space requirements

### 3. Row Store
A traditional row-based storage approach with predicate-based filtering.

**Pros:**
- Natural for record-based operations
- Good for queries that access multiple columns
- Simple to understand and implement
- Efficient for row-level operations

**Cons:**
- Higher memory usage
- Less efficient for column-oriented operations
- No data pruning capabilities

### 4. Zone Map Store
An optimized approach using zone maps to enable data pruning during query execution.

**Pros:**
- Efficient pruning of data blocks
- Reduced scan cost through zone-based filtering
- Works well with clustered data
- Maintains statistics for each zone

**Cons:**
- Additional memory overhead for zone metadata
- Less effective with uniformly distributed data
- Zone size tuning required
- Setup cost for zone creation

## Implementation Details

### Zone Map Implementation
The zone map approach divides the data into zones and maintains the following statistics for each zone:
- Min/max values for numeric columns (area)
- Value ranges for date columns (month)
- Value sets for categorical columns (town)
- Record indices for efficient access

### Zone Pruning Strategy
Zones are pruned based on three criteria:
1. Area bounds check: `if (maxArea < minRequiredArea) skip zone`
2. Month range check: `if (!monthOverlap(zoneMonths, targetMonths)) skip zone`
3. Town presence check: `if (!towns.contains(targetTown)) skip zone`

## Performance Results

### Query: Filter by Month, Town, and Area
Filtering criteria:
- Months: "2021-03" OR "2021-04"
- Town: "JURONG WEST"
- Minimum Area: 80.0 sqm

Performance metrics (average execution time):
```
Original approach: 2.00 ms
FileColumnStore approach: 5322.80 ms
RowStore approach: 9.60 ms
ZoneMap approach: 1.00 ms
```

### Memory Usage
```
Max memory: 3,948 MB
Allocated memory: 744 MB
Free memory: 425 MB
Used memory: 318 MB
```

## Analysis

### Performance Comparison
1. **ZoneMap Approach (1.00 ms)**
   - Fastest approach by a significant margin
   - Extremely effective pruning (97.78% zones skipped)
   - Minimal overhead (0.03 ms for pruning)
   - Demonstrates the power of data pruning for selective queries

2. **Original Approach (2.00 ms)**
   - Simple but surprisingly efficient
   - No overhead from complex data structures
   - Good baseline performance
   - Benefits from sequential memory access

3. **RowStore Approach (9.60 ms)**
   - Reasonable performance for row-oriented storage
   - Higher overhead due to row-based organization
   - Still maintains sub-10ms response time
   - Good for mixed workloads

4. **FileColumnStore Approach (5322.80 ms)**
   - Significantly slower due to I/O operations
   - Shows impact of disk access overhead
   - Trade-off between memory usage and speed
   - Better suited for larger-than-memory datasets

### Zone Map Effectiveness
- Zone pruning rate: 97.78% (44 out of 45 zones pruned)
- Time breakdown:
  - Pruning overhead: ~0.03 ms
  - Data scanning: ~0.38 ms
  - Total execution: ~0.43 ms

### Key Insights
1. **Pruning Efficiency**
   - Zone maps achieve nearly 98% pruning rate
   - Pruning overhead is negligible (0.03 ms)
   - Demonstrates effective zone size choice (5000 records)

2. **Memory vs. I/O Trade-off**
   - In-memory approaches (ZoneMap, Original, RowStore) significantly outperform disk-based approach
   - Memory usage remains reasonable (318 MB used)
   - Suggests dataset fits comfortably in memory

3. **Query Characteristics**
   - All approaches find exactly 209 matching records
   - High selectivity query benefits zone map approach
   - Consistent results across all implementations

## Factors Affecting Performance
1. **Data Distribution**
   - Clustered data benefits more from zone maps
   - Uniform distribution reduces pruning effectiveness

2. **Zone Size**
   - Larger zones: Less overhead, less precise pruning
   - Smaller zones: More overhead, more precise pruning
   - Current implementation uses 5000 records per zone

3. **Query Selectivity**
   - High selectivity queries benefit more from pruning
   - Low selectivity may not justify pruning overhead

## Conclusions

1. **Best Use Cases**
   - Original Approach: Simple queries, small datasets
   - File Column Store: Large datasets, limited memory
   - Row Store: Row-level operations, multiple column access
   - Zone Map: Clustered data, selective queries

2. **Performance Trade-offs**
   - Memory vs. Speed
   - Pruning Overhead vs. Scan Reduction
   - Implementation Complexity vs. Optimization Potential

3. **Recommendations**
   - Choose approach based on:
     - Data size and distribution
     - Query patterns
     - Memory constraints
     - Performance requirements

## Future Improvements

1. **Zone Map Optimizations**
   - Dynamic zone size adjustment
   - Multi-column zone statistics
   - Compressed zone metadata

2. **Hybrid Approaches**
   - Combine zone maps with columnar storage
   - Adaptive query optimization
   - Parallel processing support

## Setup and Usage

[Include setup instructions and example usage here]

## Contributing

[Include contribution guidelines here]

## License

[Include license information here]