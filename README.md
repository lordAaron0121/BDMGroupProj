# Singapore Resale Price Analysis - Performance Comparison

This project implements and compares different data storage and query optimization approaches for analyzing Singapore's resale price data. We explore various techniques to optimise the performance of filtering operations on the dataset.

## Running the Program
- To compare the different column storage solutions, run CompressionTestMain.java
- \<MatricNum>.csv output file will be created upon running the file
- (OPTIONAL) To verify that row storage indeed performs worse than the column storage solutions, run RowStoreTest.java

## Query: Filter by Month, Town, and Area
Filtering criteria:
- Months: "2016-04" OR "2016-05"
- Town: "CHOA CHU KANG"
- Minimum Area: 80.0 sqm

## Approaches Implemented

### 1. Row Store
This is simply to show the benefits of columnStore in our report. It is a row storage approach that persists in disk, using the original ResalePricesSingapore.csv

**Pros:**
- Easy to implement and understand
- Updating Row storages is less expensive
- Equal number of file reads as column-based approach for fair comparison

**Cons:**
- Higher cost because entire rows have to be read for even a single column
- Overhead from record ID management

### 2. Normal Column Store
A columnar storage implementation that persists each column to disk separately.

**Pros:**
- Reduced read time and memory usage by reading only neccessary columns
- Sequential filtering optimisation
- Persistent storage
- Good for large datasets with many columns that don't fit in memory at once

**Cons:**
- I/O overhead for column access
- Still requires sequential scan
- Disk space requirements

### 3. Compressed Column Store
Columnar storage implementation with Compressed data

**Pros:**
- Reduced disk space by storing compressed data
- Faster read times due to lower number of bytes

**Cons:**
- Overhead from mapping and un-mapping of data

### 4. Zone Map on Normal Column Store
An optimised approach using zone maps to enable data pruning during query execution.

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

### 5. Zone Map on Compressed Column Store
A hybrid approach using zone maps and compression

**Pros:**
- Reduced scan cost through zone-based filtering
- Reduced disk space by storing compressed data

**Cons:**
- Additional memory overhead for zone metadata
- Overhead from mapping and un-mapping of data


## Implementation Details

### Row Store Implementation
- The original ResalePricesSingapore.csv is used as a row-based storage
- During query execution, the file is read line-by-line, which corresponds to row-by-row

### Compression Implementation
- Each column's unique values are sorted either based on numerical order (if possible), else on lexicographical order
- They are then assigned an index starting from 0 based on the sorted order
- All values are mapped to their corresponding indexes and stored in disk

### Zone Map Implementation
The zone map approach divides the data into zones and maintains the following statistics for each zone:
- Min/max values
- Start and End byte position

### Zone Pruning Strategy
Zones are pruned based on two criteria:
1. Month range check: `if zone range covers yearMonth or nextMonth queried`
2. Town presence check: `if zone range covers town queried`