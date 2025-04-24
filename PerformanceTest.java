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
        if (args.length > 0) {
            filePath = args[0];
        }
        ColumnStore store = CSVLoader.loadCSV(filePath);
        
        // Initialize our storage implementations
        FileColumnStore fileStore = new FileColumnStore(DATA_DIR + "/columns");
        SplitRowStore splitStore = new SplitRowStore(DATA_DIR + "/split_rows");
        ZoneMapStore zoneStore = new ZoneMapStore(5000);
        
        // Load data into storage implementations
        System.out.println("Loading data into storage implementations...");
        loadDataIntoStores(store, fileStore, splitStore, zoneStore);
        
        // Print initial statistics
        System.out.println("\nDataset Statistics:");
        System.out.println("------------------");
        System.out.println("Total records: " + store.getColumn("month").size());
        printMemoryUsage();
        System.out.println();
        
        // Warmup runs
        System.out.println("Performing warmup runs...");
        for (int i = 0; i < WARMUP_RUNS; i++) {
            runSplitRowStoreApproach(splitStore);
            runFileColumnStoreApproach(fileStore);
            runZoneMapApproach(zoneStore);
            QueryCache.clear();
        }
        System.out.println("Warmup completed.\n");
        
        // Test runs
        long[] splitStoreTimes = new long[TEST_RUNS];
        long[] fileStoreTimes = new long[TEST_RUNS];
        long[] zoneMapTimes = new long[TEST_RUNS];
        int[] splitStoreMatches = new int[TEST_RUNS];
        int[] fileStoreMatches = new int[TEST_RUNS];
        int[] zoneMapMatches = new int[TEST_RUNS];
        
        System.out.println("Running performance tests...");
        for (int i = 0; i < TEST_RUNS; i++) {
            System.out.printf("Run %d/%d:\n", i + 1, TEST_RUNS);
            
            // SplitRowStore approach
            TestResult splitStoreResult = runSplitRowStoreApproach(splitStore);
            splitStoreTimes[i] = splitStoreResult.executionTime;
            splitStoreMatches[i] = splitStoreResult.matchCount;
            
            // FileColumnStore approach
            TestResult fileStoreResult = runFileColumnStoreApproach(fileStore);
            fileStoreTimes[i] = fileStoreResult.executionTime;
            fileStoreMatches[i] = fileStoreResult.matchCount;
            
            // ZoneMap approach
            TestResult zoneMapResult = runZoneMapApproach(zoneStore);
            zoneMapTimes[i] = zoneMapResult.executionTime;
            zoneMapMatches[i] = zoneMapResult.matchCount;
            
            QueryCache.clear();
            System.out.println();
        }
        
        // Print detailed results
        printDetailedResults(splitStoreTimes, fileStoreTimes, zoneMapTimes,
                           splitStoreMatches, fileStoreMatches, zoneMapMatches);
                           
        // Cleanup
        // fileStore.deleteAllFiles();
        // splitStore.deleteAllFiles();
    }

    private static void loadDataIntoStores(ColumnStore store, FileColumnStore fileStore, 
                                         SplitRowStore splitStore, ZoneMapStore zoneStore) {
        for (String key : store.getColumnNames()) {
            // Retrieve the column for the current key using store.getColumn(key)
            List<Object> columnData = store.getColumn(key);

            // Save the column data in fileStore
            fileStore.saveColumn(key, columnData);
        }
        
        String firstColumn = store.getColumnNames().iterator().next(); // Get the first column name

        // Load into RowStore
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < store.getColumn(firstColumn).size(); i++) {
            Map<String, Object> row = new HashMap<>();
            for (String key : store.getColumnNames()) {
                row.put(key, store.getColumn(key).get(i));
            }
            rows.add(row);
        }

        // Load into SplitRowStore (now splits into 4 equal parts)
        splitStore.saveData(rows);

        // Load into ZoneMapStore
        zoneStore.loadFromColumnStore(store);
    }
    
    private static TestResult runFileColumnStoreApproach(FileColumnStore store) {
        long startTime = System.nanoTime();
        
        // Define conditions for sequential filtering
        List<String> columnNames = Arrays.asList("month", "town", "floor_area_sqm");
        List<Predicate<Object>> conditions = Arrays.asList(
            month -> month.equals("2016-04") || month.equals("2016-05"),
            town -> town.equals("CHOA CHU KANG"),
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
    
    private static TestResult runZoneMapApproach(ZoneMapStore store) {
        long startTime = System.nanoTime();
        
        List<ZoneMapStore.Record> results = store.filter(
            "2016-04", "2016-05", "CHOA CHU KANG", 80.0
        );
        
        long totalTime = (System.nanoTime() - startTime) / 1_000_000;
        return new TestResult(totalTime, results.size());
    }
    
    private static TestResult runSplitRowStoreApproach(SplitRowStore store) {
        // Define conditions for filtering
        Map<String, Predicate<Object>> conditions = new HashMap<>();
        conditions.put("month", month -> month.equals("2016-04") || month.equals("2016-05"));
        conditions.put("town", town -> town.equals("CHOA CHU KANG"));
        conditions.put("floor_area_sqm", area -> Double.parseDouble((String) area) >= 80);
        
        // Minimum Price Query
        long startTime = System.nanoTime();
        List<Map<String, Object>> filteredRows = store.filter(conditions);
        double minPrice = Double.MAX_VALUE;
        for (Map<String, Object> row : filteredRows) {
            minPrice = Math.min(minPrice, Double.parseDouble((String)row.get("resale_price")));
        }
        long minPriceTime = (System.nanoTime() - startTime) / 1_000_000;
        System.out.println("Row Store - Minimum Price Query Time: " + minPriceTime + " ms");
        
        // Standard Deviation Query
        startTime = System.nanoTime();
        filteredRows = store.filter(conditions);
        double sum = 0.0;
        for (Map<String, Object> row : filteredRows) {
            sum += Double.parseDouble((String)row.get("resale_price"));
        }
        double mean = sum / filteredRows.size();
        double variance = 0.0;
        for (Map<String, Object> row : filteredRows) {
            double price = Double.parseDouble((String)row.get("resale_price"));
            variance += Math.pow(price - mean, 2);
        }
        double stdDev = Math.sqrt(variance / (filteredRows.size() - 1));
        long stdDevTime = (System.nanoTime() - startTime) / 1_000_000;
        System.out.println("Row Store - Standard Deviation Query Time: " + stdDevTime + " ms");
        
        // Average Price Query
        startTime = System.nanoTime();
        filteredRows = store.filter(conditions);
        sum = 0.0;
        for (Map<String, Object> row : filteredRows) {
            sum += Double.parseDouble((String)row.get("resale_price"));
        }
        double avgPrice = sum / filteredRows.size();
        long avgPriceTime = (System.nanoTime() - startTime) / 1_000_000;
        System.out.println("Row Store - Average Price Query Time: " + avgPriceTime + " ms");
        
        // Price per Square Meter Query
        startTime = System.nanoTime();
        filteredRows = store.filter(conditions);
        double minPricePerSqm = Double.MAX_VALUE;
        for (Map<String, Object> row : filteredRows) {
            double price = Double.parseDouble((String)row.get("resale_price"));
            double area = Double.parseDouble((String)row.get("floor_area_sqm"));
            minPricePerSqm = Math.min(minPricePerSqm, price / area);
        }
        long pricePerSqmTime = (System.nanoTime() - startTime) / 1_000_000;
        System.out.println("Row Store - Price per Square Meter Query Time: " + pricePerSqmTime + " ms");
        
        return new TestResult(minPriceTime + stdDevTime + avgPriceTime + pricePerSqmTime, filteredRows.size());
    }
    
    private static void printDetailedResults(long[] splitStoreTimes, long[] fileStoreTimes, 
                                           long[] zoneMapTimes,
                                           int[] splitStoreMatches, int[] fileStoreMatches, 
                                           int[] zoneMapMatches) {
        System.out.println("\nPerformance Results:");
        System.out.println("-------------------");
        System.out.println("SplitRowStore times: " + Arrays.toString(splitStoreTimes) + " ms");
        System.out.println("FileColumnStore times: " + Arrays.toString(fileStoreTimes) + " ms");
        System.out.println("ZoneMap times: " + Arrays.toString(zoneMapTimes) + " ms");
        
        System.out.println("\nAverage times:");
        System.out.printf("SplitRowStore approach average: %.2fms\n", average(splitStoreTimes));
        System.out.printf("FileColumnStore approach average: %.2fms\n", average(fileStoreTimes));
        System.out.printf("ZoneMap approach average: %.2fms\n", average(zoneMapTimes));
        
        System.out.println("\nMatches found:");
        System.out.println("--------------");
        System.out.printf("SplitRowStore approach: %d\n", splitStoreMatches[0]);
        System.out.printf("FileColumnStore approach: %d\n", fileStoreMatches[0]);
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

    public static String getNextMonthStr(String yearMonth) {
        // Calculate the next month for the range (manually, without using YearMonth)
        String[] parts = yearMonth.split("-");
        String year = parts[0];
        String month = parts[1];
        
        // Simple calculation for next month
        String nextMonthStr;
        if (month.equals("12")) {
            // If December, next month is January of next year
            int nextYear = Integer.parseInt(year) + 1;
            nextMonthStr = nextYear + "-01";
        } else {
            // Otherwise, just increment the month
            int nextMonth = Integer.parseInt(month) + 1;
            // Ensure two digits for month
            nextMonthStr = year + "-" + (nextMonth < 10 ? "0" + nextMonth : String.valueOf(nextMonth));
        }
        return nextMonthStr;
    }
} 