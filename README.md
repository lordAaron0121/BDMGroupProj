# Singapore Resale Price Analysis - Performance Comparison

This project implements and compares different data storage and query optimization approaches for analyzing Singapore's resale price data. We explore various techniques to optimize the performance of filtering operations on the dataset.

## Approaches Implemented

### 1. File-Based Row Store
A row storage approach that persists in disk

**Pros:**
- Easy to implement and understand
- Updating Row storages is less expensive

**Cons:**
- Higher cost because entire rows have to be read for even a single column
- Overhead from record ID management

### 2. File-Based Column Store
A columnar storage implementation that persists each column to disk separately.

**Pros:**
- Reduced read time and memory usage by reading only neccessary columns
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

### 4. Compressed File-Based Column Store
Columnar storage implementation with Compressed data

**Pros:**
- Reduced disk space by storing compressed data
- Faster read times due to lower number of bytes

**Cons:**
- Overhead from mapping and un-mapping of data


## Implementation Details

### File-Based Row Store Implementation
- Data is split into n-equal subsets, each of which are stored row-based in disk
- The value n is decided by the number of columns required for the query
- This is to allow better apple-to-apple comparison of I/O cost between column-based and row-based storage
- For example, a query has a condition using "month", and another condition using "town". A column-based storage approach that has each column stored in separate files in disk, will read only these 2 files. This row-based approach should read 2 files as well. Therefore the data is split into 2 equal subsets of rows, each stored in disk separately

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