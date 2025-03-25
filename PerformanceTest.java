import java.io.IOException;
import java.util.*;
import java.text.NumberFormat;
import java.util.function.Predicate;
import java.nio.file.Paths;

public class PerformanceTest {
    private static final int WARMUP_RUNS = 2;
    private static final int TEST_RUNS = 5;
    private static final String DATA_DIR = "data";
    
    public static void main(String[] args) throws IOException {
        // Load data
        String filePath = "../data/ResalePricesSingapore.csv";
        ColumnStore store = CSVLoader.loadCSV(filePath);
        
        // Initialize our storage implementations
        FileColumnStore fileStore = new FileColumnStore(DATA_DIR + "/columns");
        RowStore rowStore = new RowStore();
        ZoneMapStore zoneStore = new ZoneMapStore(5000);  // Changed to 5000 for better balance
        
        // Load data into storage implementations
        System.out.println("Loading data into storage implementations...");
        loadDataIntoStores(store, fileStore, rowStore, zoneStore);  // Modified to include zoneStore
        
        // Print initial statistics
        System.out.println("\nDataset Statistics:");
        System.out.println("------------------");
        System.out.println("Total records: " + store.getColumn("month").size());
        printMemoryUsage();
        System.out.println();
        
        // Warmup runs
        System.out.println("Performing warmup runs...");
        for (int i = 0; i < WARMUP_RUNS; i++) {
            runOriginalApproach(store);
            runFileColumnStoreApproach(fileStore);
            runRowStoreApproach(rowStore);
            runZoneMapApproach(zoneStore);  // Added zone map warmup
            QueryCache.clear();
        }
        System.out.println("Warmup completed.\n");
        
        // Test runs
        long[] originalTimes = new long[TEST_RUNS];
        long[] fileStoreTimes = new long[TEST_RUNS];
        long[] rowStoreTimes = new long[TEST_RUNS];
        long[] zoneMapTimes = new long[TEST_RUNS];
        int[] originalMatches = new int[TEST_RUNS];
        int[] fileStoreMatches = new int[TEST_RUNS];
        int[] rowStoreMatches = new int[TEST_RUNS];
        int[] zoneMapMatches = new int[TEST_RUNS];
        
        System.out.println("Running performance tests...");
        for (int i = 0; i < TEST_RUNS; i++) {
            System.out.printf("Run %d/%d:\n", i + 1, TEST_RUNS);
            
            // Original approach
            TestResult originalResult = runOriginalApproach(store);
            originalTimes[i] = originalResult.executionTime;
            originalMatches[i] = originalResult.matchCount;
            
            // FileColumnStore approach
            TestResult fileStoreResult = runFileColumnStoreApproach(fileStore);
            fileStoreTimes[i] = fileStoreResult.executionTime;
            fileStoreMatches[i] = fileStoreResult.matchCount;
            
            // RowStore approach
            TestResult rowStoreResult = runRowStoreApproach(rowStore);
            rowStoreTimes[i] = rowStoreResult.executionTime;
            rowStoreMatches[i] = rowStoreResult.matchCount;
            
            // ZoneMap approach
            TestResult zoneMapResult = runZoneMapApproach(zoneStore);
            zoneMapTimes[i] = zoneMapResult.executionTime;
            zoneMapMatches[i] = zoneMapResult.matchCount;
            
            QueryCache.clear();
            System.out.println();
        }
        
        // Print detailed results
        printDetailedResults(originalTimes, fileStoreTimes, rowStoreTimes, zoneMapTimes,
                           originalMatches, fileStoreMatches, rowStoreMatches, zoneMapMatches);
                           
        // Cleanup
        fileStore.deleteAllFiles();
    }

    private static void loadDataIntoStores(ColumnStore store, FileColumnStore fileStore, 
                                         RowStore rowStore, ZoneMapStore zoneStore) {
        
        for (String key : store.getColumnNames()) {
            // Retrieve the column for the current key using store.getColumn(key)
            List<Object> columnData = store.getColumn(key);

            // Save the column data in fileStore
            fileStore.saveColumn(key, columnData);
        }
        
        String firstColumn = store.getColumnNames().iterator().next(); // Get the first column name
        // Load into RowStore
        for (int i = 0; i < store.getColumn(firstColumn).size(); i++) {
            Map<String, Object> row = new HashMap<>();
            for (String key : store.getColumnNames()) {
                row.put(key, store.getColumn(key).get(i));
            }
            rowStore.addRow(row);
        }

        // Load into ZoneMapStore
        zoneStore.loadFromColumnStore(store);
    }
    
    private static TestResult runOriginalApproach(ColumnStore store) {
        final int[] matches = {0};
        List<Object> filteredPrices = new ArrayList<>();
        List<Object> filteredAreas = new ArrayList<>();
        
        long startTime = System.nanoTime();
        
        List<Object> months = store.getColumn("month");
        List<Object> towns = store.getColumn("town");
        List<Object> areas = store.getColumn("floor_area_sqm");
        List<Object> prices = store.getColumn("resale_price");
        
        for (int i = 0; i < months.size(); i++) {
            if ((months.get(i).equals("2021-03") || months.get(i).equals("2021-04")) &&
                towns.get(i).equals("JURONG WEST")) {
                double area = Double.parseDouble((String) areas.get(i));
                if (area >= 80) {
                    matches[0]++;
                    filteredPrices.add(prices.get(i));
                    filteredAreas.add(areas.get(i));
                }
            }
        }
        
        long totalTime = (System.nanoTime() - startTime) / 1_000_000;
        return new TestResult(totalTime, matches[0]);
    }
    
    private static TestResult runFileColumnStoreApproach(FileColumnStore store) {
        long startTime = System.nanoTime();
        
        // Define conditions for sequential filtering
        List<String> columnNames = Arrays.asList("month", "town", "floor_area_sqm");
        List<Predicate<Object>> conditions = Arrays.asList(
            month -> month.equals("2021-03") || month.equals("2021-04"),
            town -> town.equals("JURONG WEST"),
            area -> Double.parseDouble((String) area) >= 80
        );
        
        // Get qualified indexes
        List<Integer> qualifiedIndexes = store.sequentialFilter(columnNames, conditions);
        
        // Get prices and areas for qualified records
        List<Object> prices = store.readColumn("resale_price");
        List<Object> areas = store.readColumn("floor_area_sqm");
        
        List<Object> filteredPrices = new ArrayList<>();
        List<Object> filteredAreas = new ArrayList<>();
        for (Integer index : qualifiedIndexes) {
            filteredPrices.add(prices.get(index));
            filteredAreas.add(areas.get(index));
        }
        
        long totalTime = (System.nanoTime() - startTime) / 1_000_000;
        return new TestResult(totalTime, qualifiedIndexes.size());
    }
    
    private static TestResult runRowStoreApproach(RowStore store) {
        long startTime = System.nanoTime();
        
        // Define conditions for filtering
        Map<String, Predicate<Object>> conditions = new HashMap<>();
        conditions.put("month", month -> month.equals("2021-03") || month.equals("2021-04"));
        conditions.put("town", town -> town.equals("JURONG WEST"));
        conditions.put("floor_area_sqm", area -> Double.parseDouble((String) area) >= 80);
        
        // Filter rows
        List<Map<String, Object>> filteredRows = store.filter(conditions);
        
        // Extract prices and areas
        List<Object> filteredPrices = new ArrayList<>();
        List<Object> filteredAreas = new ArrayList<>();
        for (Map<String, Object> row : filteredRows) {
            filteredPrices.add(row.get("resale_price"));
            filteredAreas.add(row.get("floor_area_sqm"));
        }
        
        long totalTime = (System.nanoTime() - startTime) / 1_000_000;
        return new TestResult(totalTime, filteredRows.size());
    }
    
    private static TestResult runZoneMapApproach(ZoneMapStore store) {
        long startTime = System.nanoTime();
        
        List<ZoneMapStore.Record> results = store.filter(
            "2021-03", "2021-04", "JURONG WEST", 80.0
        );
        
        long totalTime = (System.nanoTime() - startTime) / 1_000_000;
        return new TestResult(totalTime, results.size());
    }
    
    private static void printDetailedResults(long[] originalTimes, long[] fileStoreTimes, 
                                           long[] rowStoreTimes, long[] zoneMapTimes,
                                           int[] originalMatches, int[] fileStoreMatches, 
                                           int[] rowStoreMatches, int[] zoneMapMatches) {
        System.out.println("\nPerformance Results:");
        System.out.println("-------------------");
        System.out.println("Original approach times: " + Arrays.toString(originalTimes) + " ms");
        System.out.println("FileColumnStore times: " + Arrays.toString(fileStoreTimes) + " ms");
        System.out.println("RowStore times: " + Arrays.toString(rowStoreTimes) + " ms");
        System.out.println("ZoneMap times: " + Arrays.toString(zoneMapTimes) + " ms");
        
        System.out.println("\nAverage times:");
        System.out.printf("Original approach average: %.2fms\n", average(originalTimes));
        System.out.printf("FileColumnStore approach average: %.2fms\n", average(fileStoreTimes));
        System.out.printf("RowStore approach average: %.2fms\n", average(rowStoreTimes));
        System.out.printf("ZoneMap approach average: %.2fms\n", average(zoneMapTimes));
        
        System.out.println("\nMatches found:");
        System.out.println("--------------");
        System.out.printf("Original approach: %d\n", originalMatches[0]);
        System.out.printf("FileColumnStore approach: %d\n", fileStoreMatches[0]);
        System.out.printf("RowStore approach: %d\n", rowStoreMatches[0]);
        System.out.printf("ZoneMap approach: %d\n", zoneMapMatches[0]);
        
        System.out.println("\nMemory Usage:");
        System.out.println("-------------");
        printMemoryUsage();
    }
    
    private static void printMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        NumberFormat format = NumberFormat.getInstance();
        
        long maxMemory = runtime.maxMemory();
        long allocatedMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        
        System.out.println("\nMemory Usage:");
        System.out.println("  Max memory: " + format.format(maxMemory / 1024 / 1024) + "MB");
        System.out.println("  Allocated memory: " + format.format(allocatedMemory / 1024 / 1024) + "MB");
        System.out.println("  Free memory: " + format.format(freeMemory / 1024 / 1024) + "MB");
        System.out.println("  Used memory: " + format.format((allocatedMemory - freeMemory) / 1024 / 1024) + "MB");
    }
    
    private static double average(long[] values) {
        long sum = 0;
        for (long value : values) {
            sum += value;
        }
        return (double) sum / values.length;
    }
    
    private static class TestResult {
        final long executionTime;
        final int matchCount;
        
        TestResult(long executionTime, int matchCount) {
            this.executionTime = executionTime;
            this.matchCount = matchCount;
        }
    }
} 