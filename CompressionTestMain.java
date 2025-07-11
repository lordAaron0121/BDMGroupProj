// Main.java
import java.io.IOException;
import java.util.Map;
import java.io.File;

public class CompressionTestMain {
    public static void main(String[] args) {
        try {
            String csvFilePath = "../data/ResalePricesSingapore.csv";
            
            // Directories to store column files
            String normalColumnStoreDir = "normal_column_store";
            String compressedColumnStoreDir = "compressed_column_store";
            
            // Print working directory for debugging
            System.out.println("Current working directory: " + new File(".").getAbsolutePath());
            
            // Validate file path
            File csvFile = new File(csvFilePath);
            if (!csvFile.exists() || !csvFile.canRead()) {
                System.err.println("Error: Cannot access the CSV file at: " + csvFile.getAbsolutePath());
                // Try alternate path
                csvFilePath = "data/ResalePricesSingapore.csv";
                csvFile = new File(csvFilePath);
                if (!csvFile.exists() || !csvFile.canRead()) {
                    System.err.println("Error: Cannot access the alternate CSV file at: " + csvFile.getAbsolutePath());
                    System.exit(1);
                }
            }
            
            System.out.println("=== PERFORMANCE ANALYSIS: NORMAL VS COMPRESSED COLUMN STORE ===");
            
            // Parameters for queries
            String yearMonth = "2016-04";
            String town = "CHOA CHU KANG";
            
            // 1. Memory Analysis - Load both column stores and compare memory usage
            System.out.println("\n--- MEMORY USAGE ANALYSIS ---");
            
            // Normal Column Store
            System.out.println("\nInitializing normal column store...");
            Runtime.getRuntime().gc(); // Request garbage collection before measurement
            long normalStartMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            
            NormalColumnStore normalStore = new NormalColumnStore(normalColumnStoreDir);
            System.out.println("Loading data from CSV file into normal column store: " + csvFile.getAbsolutePath());
            normalStore.loadFromCSV(csvFilePath);
            
            Runtime.getRuntime().gc(); // Request garbage collection after loading
            long normalEndMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            long normalMemoryUsed = normalEndMemory - normalStartMemory;
            
            System.out.println("Normal column store created successfully!");
            System.out.println("Normal column store memory usage: " + formatMemorySize(normalMemoryUsed));

            normalStore.generateZoneMapsFromColumns(800);
            
            // Compressed Column Store
            System.out.println("\nInitializing compressed column store...");
            Runtime.getRuntime().gc(); // Request garbage collection before measurement
            long compressedStartMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            
            CompressedColumnStore compressedStore = new CompressedColumnStore(compressedColumnStoreDir);
            System.out.println("Loading data from CSV file into compressed column store: " + csvFile.getAbsolutePath());
            compressedStore.loadFromCSV(csvFilePath);
            
            Runtime.getRuntime().gc(); // Request garbage collection after loading
            long compressedEndMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            long compressedMemoryUsed = compressedEndMemory - compressedStartMemory;
            
            System.out.println("Compressed column store created successfully!");
            System.out.println("Compressed column store memory usage: " + formatMemorySize(compressedMemoryUsed));

            compressedStore.generateZoneMapsFromCompressedColumns(800);
            
            // Memory comparison
            double memoryReductionPercent = 100.0 * (normalMemoryUsed - compressedMemoryUsed) / normalMemoryUsed;
            System.out.println("\nMemory reduction with compression: " + String.format("%.2f%%", memoryReductionPercent));
            System.out.println("Memory saved: " + formatMemorySize(normalMemoryUsed - compressedMemoryUsed));
            
            // 2. Query Time Analysis
            System.out.println("\n--- QUERY PERFORMANCE ANALYSIS ---");
            System.out.println("Running queries for " + yearMonth + " in " + town);
            
            // Initialize query engines
            NormalQueryEngine normalQueryEngine = new NormalQueryEngine(normalStore);
            CompressedQueryEngine compressedQueryEngine = new CompressedQueryEngine(compressedStore);
            
            // Run normal queries and measure time
            System.out.println("\nRunning queries on normal column store...");
            long normalQueryStartTime = System.nanoTime();
            Map<String, Object> normalResults = normalQueryEngine.runAllQueries(yearMonth, town);
            long normalQueryEndTime = System.nanoTime();
            long normalQueryTime = normalQueryEndTime - normalQueryStartTime;

            // Run normal queries with Zone maps and measure time
            System.out.println("\nRunning queries on normal column store with Zone Map...");
            long normalQueryZoneMapStartTime = System.nanoTime();
            Map<String, Object> normalZoneMapResults = normalQueryEngine.runAllQueriesZoneMap(yearMonth, town);
            long normalQueryZoneMapEndTime = System.nanoTime();
            long normalQueryZoneMapTime = normalQueryZoneMapEndTime - normalQueryZoneMapStartTime;
            
            // Run compressed queries and measure time
            System.out.println("\nRunning queries on compressed column store...");
            long compressedQueryStartTime = System.nanoTime();
            Map<String, Object> compressedResults = compressedQueryEngine.runAllQueries(yearMonth, town);
            long compressedQueryEndTime = System.nanoTime();
            long compressedQueryTime = compressedQueryEndTime - compressedQueryStartTime;

            // Run compressed queries with Zone Map and measure time
            System.out.println("\nRunning queries on compressed column store...");
            long compressedQueryZoneMapStartTime = System.nanoTime();
            Map<String, Object> compressedZoneMapResults = compressedQueryEngine.runAllQueriesZoneMap(yearMonth, town);
            long compressedQueryZoneMapEndTime = System.nanoTime();
            long compressedQueryZoneMapTime = compressedQueryZoneMapEndTime - compressedQueryZoneMapStartTime;

            // Time comparison
            System.out.println("\n--- QUERY TIME COMPARISON ---");
            System.out.println("Normal column store query time: " + formatTime(normalQueryTime));
            System.out.println("Normal column store with Zone Map query time: " + formatTime(normalQueryZoneMapTime));
            System.out.println("Compressed column store query time: " + formatTime(compressedQueryTime));
            System.out.println("Compressed column store with Zone Map query time: " + formatTime(compressedQueryZoneMapTime));
            
            double timeRatioPercent = 100.0 * compressedQueryTime / normalQueryTime;
            System.out.println("Compressed/Normal time ratio: " + String.format("%.2f%%", timeRatioPercent));
            
            if (compressedQueryTime > normalQueryTime) {
                double slowdownPercent = timeRatioPercent - 100.0;
                System.out.println("Query slowdown with compression: " + String.format("%.2f%%", slowdownPercent));
            } else {
                double speedupPercent = 100.0 - timeRatioPercent;
                System.out.println("Query speedup with compression: " + String.format("%.2f%%", speedupPercent));
            }
            
            // 3. Offline Memory Analysis
            performDetailedMemoryAnalysis(normalColumnStoreDir, compressedColumnStoreDir);       
            
            // 4. Print query results for verification
            System.out.println("\n--- QUERY RESULTS VERIFICATION ---");
            
            System.out.println("\nNormal Column Store Results:");
            printQueryResults(normalResults);

            System.out.println("\nNormal Column Store With ZoneMap Results:");
            printQueryResults(normalZoneMapResults);
            
            System.out.println("\nCompressed Column Store Results:");
            printQueryResults(compressedResults);

            System.out.println("\nCompressed Column Store with ZoneMap Results:");
            printQueryResults(compressedZoneMapResults);
            
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void performDetailedMemoryAnalysis(String normalColumnStoreDir, String compressedColumnStoreDir) {
        System.out.println("\n--- DETAILED COLUMN-BY-COLUMN MEMORY ANALYSIS ---");
        
        File normalDir = new File(normalColumnStoreDir);
        File compressedDir = new File(compressedColumnStoreDir);
        
        if (!normalDir.exists() || !normalDir.isDirectory() || !compressedDir.exists() || !compressedDir.isDirectory()) {
            System.err.println("Error: One or both column store directories do not exist.");
            return;
        }
        
        // Get all .col files in normal column store directory
        File[] normalFiles = normalDir.listFiles((dir, name) -> name.endsWith(".col"));
        if (normalFiles == null || normalFiles.length == 0) {
            System.err.println("Error: No .col files found in normal column store directory.");
            return;
        }
        
        long totalNormalSize = 0;
        long totalCompressedSize = 0;
        
        for (File normalFile : normalFiles) {
            String columnName = normalFile.getName().replace(".col", "");
            long normalSize = normalFile.length();
            totalNormalSize += normalSize;
            
            // Find corresponding compressed file and dictionary file
            File compressedFile = new File(compressedDir, columnName + ".cmp");
            File dictFile = new File(compressedDir, columnName + ".dict");
            
            long compressedSize = 0;
            if (compressedFile.exists()) {
                compressedSize += compressedFile.length();
            } else {
                System.out.println("Warning: No compressed file found for column: " + columnName);
            }
            
            // Add dictionary size if it exists
            if (dictFile.exists()) {
                compressedSize += dictFile.length();
            }
            
            totalCompressedSize += compressedSize;
            
            // Calculate metrics
            double reductionPercent = 100.0 * (normalSize - compressedSize) / normalSize;
            long memorySaved = normalSize - compressedSize;
            
            // Print column analysis
            System.out.printf("%s: compressed memory = %s | non compressed memory = %s | Memory reduction with compression: %.2f%% | Memory saved: %s%n",
                    columnName,
                    formatMemorySize(compressedSize),
                    formatMemorySize(normalSize),
                    reductionPercent,
                    formatMemorySize(memorySaved));
        }
        
        // Print total analysis
        double totalReductionPercent = 100.0 * (totalNormalSize - totalCompressedSize) / totalNormalSize;
        long totalMemorySaved = totalNormalSize - totalCompressedSize;
        
        System.out.println("\nTOTAL: compressed memory = " + formatMemorySize(totalCompressedSize) +
                " | non compressed memory = " + formatMemorySize(totalNormalSize) +
                " | Memory reduction with compression: " + String.format("%.2f%%", totalReductionPercent) +
                " | Memory saved: " + formatMemorySize(totalMemorySaved));
    }

    public static String formatDollar(String value) {
        try {
            double num = Double.parseDouble(value);
            return String.format("%.2f", num);
        } catch (NumberFormatException e) {
            return value; // Return as-is if it's not a number
        }
    }

    public static void printResult(Map<String, String> results, Map<String, Double> timings, String query) {
        System.out.printf("%-32s %-12s | Duration: %6.2f ms\n", query, formatDollar(results.get(query)), timings.get(query));
    }
    
    @SuppressWarnings("unchecked")
    public static void printQueryResults(Map<String, Object> resultsAndTimings) {
        // Get the results map
        Map<String, String> results = (Map<String, String>) resultsAndTimings.get("results");

        // Get the timings map
        Map<String, Double> timings = (Map<String, Double>) resultsAndTimings.get("timings");


        // Output the result with the duration, ensuring aligned columns
        printResult(results, timings, "Minimum Price");
        printResult(results, timings, "Standard Deviation of Price");
        printResult(results, timings, "Average Price");
        printResult(results, timings, "Minimum Price per Square Meter");
    }
    
    public static String formatMemorySize(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", bytes / (1024.0 * 1024));
        } else {
            return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
        }
    }
    
    public static String formatTime(long nanoTime) {
        if (nanoTime < 1000) {
            return nanoTime + " ns";
        } else if (nanoTime < 1000 * 1000) {
            return String.format("%.2f μs", nanoTime / 1000.0);
        } else if (nanoTime < 1000 * 1000 * 1000) {
            return String.format("%.2f ms", nanoTime / (1000.0 * 1000));
        } else {
            return String.format("%.2f s", nanoTime / (1000.0 * 1000 * 1000));
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