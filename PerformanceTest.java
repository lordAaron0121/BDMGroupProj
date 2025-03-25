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
        // Get all columns
        List<Object> months = store.getColumn("month");
        List<Object> towns = store.getColumn("town");
        List<Object> areas = store.getColumn("floor_area_sqm");
        List<Object> prices = store.getColumn("resale_price");
        
        // Load into FileColumnStore
        fileStore.saveColumn("month", months);
        fileStore.saveColumn("town", towns);
        fileStore.saveColumn("floor_area_sqm", areas);
        fileStore.saveColumn("resale_price", prices);
        
        // Create row data
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < months.size(); i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("month", months.get(i));
            row.put("town", towns.get(i));
            row.put("floor_area_sqm", areas.get(i));
            row.put("resale_price", prices.get(i));
            rows.add(row);
        }

        // Load into SplitRowStore
        splitStore.createGroup("time_location", Arrays.asList("month", "town"));
        splitStore.createGroup("area", Arrays.asList("floor_area_sqm"));
        splitStore.createGroup("price", Arrays.asList("resale_price"));
        splitStore.saveData("time_location", rows);
        splitStore.saveData("area", rows);
        splitStore.saveData("price", rows);

        // Load into ZoneMapStore
        zoneStore.loadFromColumnStore(store);
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
    
    private static TestResult runZoneMapApproach(ZoneMapStore store) {
        long startTime = System.nanoTime();
        
        List<ZoneMapStore.Record> results = store.filter(
            "2021-03", "2021-04", "JURONG WEST", 80.0
        );
        
        long totalTime = (System.nanoTime() - startTime) / 1_000_000;
        return new TestResult(totalTime, results.size());
    }
    
    private static TestResult runSplitRowStoreApproach(SplitRowStore store) {
        long startTime = System.nanoTime();
        
        // First filter by month and town using the time_location group
        Map<String, Predicate<Object>> timeLocationConditions = new HashMap<>();
        timeLocationConditions.put("month", month -> month.equals("2021-03") || month.equals("2021-04"));
        timeLocationConditions.put("town", town -> town.equals("JURONG WEST"));
        List<Map<String, Object>> timeLocationFiltered = store.filter("time_location", timeLocationConditions);
        
        // Then filter by area using the area group
        Map<String, Predicate<Object>> areaConditions = new HashMap<>();
        areaConditions.put("floor_area_sqm", area -> Double.parseDouble((String) area) >= 80);
        List<Map<String, Object>> areaFiltered = store.filter("area", areaConditions);
        
        // Create maps for quick lookups
        Map<Integer, Map<String, Object>> timeLocationById = new HashMap<>();
        Map<Integer, Map<String, Object>> areaById = new HashMap<>();
        Map<Integer, Map<String, Object>> priceById = new HashMap<>();
        
        for (Map<String, Object> row : timeLocationFiltered) {
            timeLocationById.put((Integer) row.get("_id"), row);
        }
        
        for (Map<String, Object> row : areaFiltered) {
            areaById.put((Integer) row.get("_id"), row);
        }
        
        // Get prices for all records
        List<Map<String, Object>> priceData = store.readGroup("price");
        for (Map<String, Object> row : priceData) {
            priceById.put((Integer) row.get("_id"), row);
        }
        
        // Find records that match all conditions
        List<Object> filteredPrices = new ArrayList<>();
        List<Object> filteredAreas = new ArrayList<>();
        Set<Integer> matchingIds = new HashSet<>(timeLocationById.keySet());
        matchingIds.retainAll(areaById.keySet());
        
        // Collect results in order
        for (Integer id : matchingIds) {
            if (timeLocationById.containsKey(id) && areaById.containsKey(id) && priceById.containsKey(id)) {
                filteredPrices.add(priceById.get(id).get("resale_price"));
                filteredAreas.add(areaById.get(id).get("floor_area_sqm"));
            }
        }
        
        long totalTime = (System.nanoTime() - startTime) / 1_000_000;
        return new TestResult(totalTime, matchingIds.size());
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
} 