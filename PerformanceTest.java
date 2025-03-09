import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.text.NumberFormat;

public class PerformanceTest {
    private static final int WARMUP_RUNS = 2;
    private static final int TEST_RUNS = 5;
    
    public static void main(String[] args) throws IOException {
        // Load data
        String filePath = "../data/ResalePricesSingapore.csv";
        ColumnStore store = CSVLoader.loadCSV(filePath);
        
        // Print initial statistics
        System.out.println("Dataset Statistics:");
        System.out.println("------------------");
        System.out.println("Total records: " + store.getColumn("month").size());
        printMemoryUsage();
        System.out.println();
        
        // Warmup runs
        System.out.println("Performing warmup runs...");
        for (int i = 0; i < WARMUP_RUNS; i++) {
            runOriginalApproach(store);
            runOptimizedApproach(store);
            QueryCache.clear();
        }
        System.out.println("Warmup completed.\n");
        
        // Test runs
        long[] originalTimes = new long[TEST_RUNS];
        long[] optimizedTimes = new long[TEST_RUNS];
        int[] originalMatches = new int[TEST_RUNS];
        int[] optimizedMatches = new int[TEST_RUNS];
        
        System.out.println("Running performance tests...");
        for (int i = 0; i < TEST_RUNS; i++) {
            System.out.printf("Run %d/%d:\n", i + 1, TEST_RUNS);
            
            // Original approach
            TestResult originalResult = runOriginalApproach(store);
            originalTimes[i] = originalResult.executionTime;
            originalMatches[i] = originalResult.matchCount;
            
            // Optimized approach
            TestResult optimizedResult = runOptimizedApproach(store);
            optimizedTimes[i] = optimizedResult.executionTime;
            optimizedMatches[i] = optimizedResult.matchCount;
            
            QueryCache.clear();
            System.out.println();
        }
        
        // Print detailed results
        printDetailedResults(originalTimes, optimizedTimes, originalMatches, optimizedMatches);
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
    
    private static TestResult runOptimizedApproach(ColumnStore store) {
        final int[] matches = {0};
        List<Object> filteredPrices = new ArrayList<>();
        List<Object> filteredAreas = new ArrayList<>();
        
        long startTime = System.nanoTime();
        
        // Get all columns at once to avoid multiple lookups
        List<Object> months = store.getColumn("month");
        
        // Only collect months that pass the month condition
        List<Integer> validMonths = new ArrayList<>();
        for (int i = 0; i < months.size(); i++) {
            if (months.get(i).equals("2021-03") || months.get(i).equals("2021-04")) {
                validMonths.add(i);
            }
        }

        List<Object> towns = store.getColumn("town");

        // Only collect towns that correspond to the valid months
        List<Integer> validTowns = new ArrayList<>();
        for (int i : validMonths) {
            if (towns.get(i).equals("JURONG WEST")) {
                validTowns.add(i);
            }
        }

        List<Object> areas = store.getColumn("floor_area_sqm");

        // Only collect areas that correspond to the valid months and towns
        List<Integer> validAreas = new ArrayList<>();
        for (int i : validTowns) {
            if (Double.parseDouble((String) areas.get(i))  >= 80) {
                    validAreas.add(i);
                }
            }
        
        List<Object> prices = store.getColumn("resale_price");

        // Now, process the prices that correspond to the valid rows from the previous filters
        for (int i : validAreas) {
            filteredPrices.add(prices.get(i));
            filteredAreas.add(areas.get(i));
            matches[0]++;
            }
        
        long totalTime = (System.nanoTime() - startTime) / 1_000_000;
        
        // Print statistics after timing (unchanged)
        if (matches[0] > 0) {
            System.out.println("\nHDB Resale Statistics (Area >= 80 sq m):");
            System.out.println("----------------------------------------");
            double minPrice = QueryProcessor.min(filteredPrices);
            double avgPrice = QueryProcessor.average(filteredPrices);
            double stdDev = QueryProcessor.standardDeviation(filteredPrices);
            double minPricePerSqm = QueryProcessor.minPricePerSquareMeter(filteredPrices, filteredAreas);
            
            System.out.printf("Minimum Price: $%.2f\n", minPrice);
            System.out.printf("Average Price: $%.2f\n", avgPrice);
            System.out.printf("Standard Deviation of Price: $%.2f\n", stdDev);
            System.out.printf("Minimum Price per Square Meter: $%.2f\n", minPricePerSqm);
            System.out.printf("Number of matching flats: %d\n\n", matches[0]);
            
            // Validation output
            System.out.println("Debug Information:");
            System.out.println("------------------");
            System.out.println("First 5 matching records:");
            for (int i = 0; i < Math.min(5, filteredPrices.size()); i++) {
                System.out.printf("Price: $%s, Area: %s sq m\n", 
                    filteredPrices.get(i), 
                    filteredAreas.get(i));
            }
        }
        
        return new TestResult(totalTime, matches[0]);
    }
    
    private static void printDetailedResults(long[] originalTimes, long[] optimizedTimes, 
                                          int[] originalMatches, int[] optimizedMatches) {
        System.out.println("\nDetailed Performance Results:");
        System.out.println("---------------------------");
        
        // Calculate averages
        double avgOriginal = average(originalTimes);
        double avgOptimized = average(optimizedTimes);
        double improvement = ((avgOriginal - avgOptimized) / avgOriginal) * 100;
        
        // Print timing results
        System.out.println("Original Approach:");
        System.out.printf("  Average time: %.2fms\n", avgOriginal);
        System.out.printf("  Min time: %dms\n", min(originalTimes));
        System.out.printf("  Max time: %dms\n", max(originalTimes));
        System.out.printf("  Std Dev: %.2fms\n", standardDeviation(originalTimes));
        System.out.printf("  Matches found: %d\n", originalMatches[0]);
        
        System.out.println("\nOptimized Approach:");
        System.out.printf("  Average time: %.2fms\n", avgOptimized);
        System.out.printf("  Min time: %dms\n", min(optimizedTimes));
        System.out.printf("  Max time: %dms\n", max(optimizedTimes));
        System.out.printf("  Std Dev: %.2fms\n", standardDeviation(optimizedTimes));
        System.out.printf("  Matches found: %d\n", optimizedMatches[0]);
        
        System.out.printf("\nOverall improvement: %.2f%%\n", improvement);
        
        // Print final memory usage
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
    
    private static long min(long[] values) {
        long min = values[0];
        for (long value : values) {
            if (value < min) min = value;
        }
        return min;
    }
    
    private static long max(long[] values) {
        long max = values[0];
        for (long value : values) {
            if (value > max) max = value;
        }
        return max;
    }
    
    private static double standardDeviation(long[] values) {
        double avg = average(values);
        double sumSquaredDiff = 0;
        for (long value : values) {
            sumSquaredDiff += Math.pow(value - avg, 2);
        }
        return Math.sqrt(sumSquaredDiff / values.length);
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