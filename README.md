# Singapore Resale Price Analysis - Performance Comparison

This project implements and compares different data storage and query optimization approaches for analyzing Singapore's resale price data. We explore various techniques to optimize the performance of filtering operations on the dataset.

## Approaches Implemented

### 1. Split Row Store
A hybrid approach that splits row-based data into multiple files based on query access patterns.

**Pros:**
- Reduced I/O by only reading relevant columns
- Better cache utilization with smaller files
- Flexible grouping based on query patterns
- Potential for parallel processing

**Cons:**
- Multiple file I/O operations
- Overhead from record ID management
- Complex implementation
- Storage space overhead

### 2. File-Based Column Store
A columnar storage implementation that persists each column to disk separately.

**Pros:**
- Reduced memory usage through file-based storage
- Sequential filtering optimization
- Persistent storage
- Good for large datasets that don't fit in memory

**Cons:**
- I/O overhead for column access
- Still requires sequential scan
- Disk space requirements

### 3. Zone Map Store
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

### Split Row Store Implementation
- Data is split into logical groups based on query patterns
- Each group contains a subset of columns stored in row format
- Record IDs maintain relationships between split files
- Optimized for specific query patterns

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