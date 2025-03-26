# Singapore Resale Price Analysis - Performance Comparison

This project implements and compares different data storage and query optimization approaches for analyzing Singapore's resale price data. We explore various techniques to optimize the performance of filtering operations on the dataset.

## Approaches Implemented

### 1. File-Based Row Store
A row storage approach that persists in disk

**Pros:**
- Easy to implement and understand
- Updating Row storages is less expensive
- Equal number of file reads as column-based approach for fair comparison

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
- Data is split into 4 equal subsets of rows, each stored in a separate file
- This matches the number of files read in the column-based approach (4 columns: month, town, area, price)
- Each file contains complete rows with all columns
- During query execution, all 4 files are read and filtered in parallel
- This provides a fair comparison with the column-based approach in terms of I/O operations

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

## Recent Updates and Improvements

### Row Store Optimization (Latest Update)
We have improved the row-based storage implementation to provide a more fair comparison with the column-based approach:

1. **Equal File Distribution**
   - Data is now split into exactly 4 files, matching the number of columns used in queries
   - Each file contains approximately equal number of rows
   - All columns are preserved in each file for complete row access

2. **Implementation Details**
   - Removed the previous group-based storage approach
   - Implemented automatic data partitioning into 4 equal parts
   - Fixed serialization issues with Java's SubList by creating proper ArrayList copies
   - Improved error handling and file management

3. **Performance Considerations**
   - Both row and column approaches now read exactly 4 files during query execution
   - This provides a more accurate I/O comparison between approaches
   - The row-based approach can potentially benefit from parallel file reading
   - Memory usage is more evenly distributed across files

4. **Trade-offs**
   - Pros:
     - More balanced I/O operations
     - Better comparison baseline with column-based approach
     - Potential for parallel processing
   - Cons:
     - Still needs to read complete rows
     - Additional storage overhead from column duplication
     - Complexity in maintaining file boundaries

This update ensures that our performance comparisons between row-based and column-based approaches are more meaningful by equalizing the number of file operations required for each query.