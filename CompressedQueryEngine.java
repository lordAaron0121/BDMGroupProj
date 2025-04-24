import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.nio.file.*;

public class CompressedQueryEngine {
    private CompressedColumnStore columnStore;
    
    public CompressedQueryEngine(CompressedColumnStore columnStore) {
        this.columnStore = columnStore;
    }

    public List<Integer> getSubsetByMonthAndTown(String yearMonth, String town) throws IOException {
        List<String> months = columnStore.getColumnData("month");
        List<String> towns = columnStore.getColumnData("town");
        List<String> floor = columnStore.getColumnData("floor_area_sqm");
        List<Integer> matchingIndices = new ArrayList<>();

        String nextMonthStr = PerformanceTest.getNextMonthStr(yearMonth);
        
        System.out.println("Filtering transactions with month = " + yearMonth + " OR month = " + nextMonthStr + " for town: " + town);
        
        // Find all rows that match the criteria
        for (int i = 0; i < months.size(); i++) {
            String monthValue = months.get(i);
            String townValue = towns.get(i);
            Double floorValue = Double.parseDouble(floor.get(i));
            
            // Simple string matching: month equals yearMonth OR month equals nextMonthStr
            if ((monthValue.equals(yearMonth) || monthValue.equals(nextMonthStr)) && 
                townValue.equals(town) &&
                floorValue >= 80) {
                matchingIndices.add(i);
            }
        }
        
        System.out.println("Found " + matchingIndices.size() + " matching transactions");
        return matchingIndices;
    }
    
    /**
     * Optimized version to get subset when we know month and town are compressed.
     * This directly uses the dictionary files without fully decompressing the data.
     */
    public List<Integer> getSubsetByMonthAndTownOptimized(String yearMonth, String town) throws IOException {
        List<Integer> matchingIndices = new ArrayList<>();
        
        String nextMonthStr = PerformanceTest.getNextMonthStr(yearMonth);
        
        System.out.println("Filtering transactions with month = " + yearMonth + " OR month = " + nextMonthStr + " for town: " + town);

        try {
            // Check if we can use the optimized path with compressed dictionaries
            Map<String, Integer> monthDict = loadDictionary("month");
            Map<String, Integer> townDict = loadDictionary("town");
            Map<String, Integer> floor_area_sqmDict = loadDictionary("floor_area_sqm");
            
            if (monthDict != null && townDict != null && floor_area_sqmDict != null) {
                // Get the indices for our target values
                Integer monthIndex1 = monthDict.get(yearMonth);
                Integer monthIndex2 = monthDict.get(nextMonthStr);
                Integer townIndex = townDict.get(town);
                
                // If any value doesn't exist in the dictionary, we can't use this optimization
                if ((monthIndex1 == null && monthIndex2 == null) || townIndex == null) {
                    // Fallback to regular method
                    return getSubsetByMonthAndTown(yearMonth, town);
                }
                
                // Get compressed data
                byte[] monthData = loadCompressedData("month");
                byte[] townData = loadCompressedData("town");
                byte[] floor_area_sqmData = loadCompressedData("floor_area_sqm");
                
                if (monthData != null && townData != null) {
                    int recordCount = monthDict.get("# Number of records");
                    int monthBits = monthDict.get("# Bits used per value");
                    int townBits = townDict.get("# Bits used per value");
                    int floor_area_sqmBits = floor_area_sqmDict.get("# Bits used per value");

                    // Process each record without fully decompressing
                    BitStreamReader monthReader = new BitStreamReader(monthData, monthBits);
                    BitStreamReader townReader = new BitStreamReader(townData, townBits);
                    BitStreamReader floor_area_sqmReader = new BitStreamReader(floor_area_sqmData, floor_area_sqmBits);

                    // Skip metadata in both readers
                    monthReader.skipMetadata();
                    townReader.skipMetadata();
                    floor_area_sqmReader.skipMetadata();
                    Map<Integer, String> reversedMap = reverseMap(floor_area_sqmDict);
                    
                    for (int i = 0; i < recordCount; i++) {
                        int monthValue = monthReader.readBits();
                        int townValue = townReader.readBits();
                        int floor_area_sqmTemp = floor_area_sqmReader.readBits();
                        
                        if ((monthValue == monthIndex1 || monthValue == monthIndex2) && 
                            townValue == townIndex) {
                            double floor_area_sqmValue = Double.parseDouble(reversedMap.get(floor_area_sqmTemp));
                            if (floor_area_sqmValue >= 80) {
                                matchingIndices.add(i);
                            }
                        }
                    }

                    System.out.println("Found " + matchingIndices.size() + " matching transactions (optimized)");
                    return matchingIndices;
                }
            }
        } catch (Exception e) {
            System.err.println("Optimization failed, falling back to regular method: " + e.getMessage());
            // Fall back to regular method
        }
        
        // If optimization failed or wasn't possible, use the regular method
        return getSubsetByMonthAndTown(yearMonth, town);
    }
    
    /**
     * Helper method to load a dictionary from disk
     */
    private Map<String, Integer> loadDictionary(String columnName) {
        try {
            String dictionaryPath = columnStore.getDataDirectory() + java.io.File.separator + columnName + ".dict";
            java.io.File dictFile = new java.io.File(dictionaryPath);
            
            if (!dictFile.exists()) {
                return null;  // Dictionary doesn't exist
            }
            
            Map<String, Integer> dictionary = new HashMap<>();
            try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(dictionaryPath))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("#")) {
                        if (line.startsWith("# Bits used per value:")) {
                            dictionary.put("# Bits used per value", Integer.parseInt(line.split(":")[1].trim()));
                        }
                        else if (line.startsWith("# Number of records:")) {
                            dictionary.put("# Number of records", Integer.parseInt(line.split(":")[1].trim()));
                        }
                    continue;
                    }
                    
                    String[] parts = line.split(",");
                    if (parts.length >= 2) {
                        dictionary.put(parts[0], Integer.parseInt(parts[1]));
                    }
                }
            }
            
            return dictionary;
        } catch (Exception e) {
            return null;  // If any error occurs, return null
        }
    }
    
    public List<String> readAndUncompressData(String columnName) throws IOException {
        try {
            // Check if we can use the optimized path with compressed dictionaries
            Map<String, Integer> columnDict = loadDictionary(columnName);
                
            // Get compressed data
            byte[] columnData = loadCompressedData(columnName);
            
            int recordCount = columnDict.get("# Number of records");
            int columnBits = columnDict.get("# Bits used per value");

            // Process each record without fully decompressing
            BitStreamReader columnReader = new BitStreamReader(columnData, columnBits);

            // Skip metadata in both readers
            columnReader.skipMetadata();
            Map<Integer, String> reversedMap = reverseMap(columnDict);
            
            List<String> result = new ArrayList<>();
            for (int i = 0; i < recordCount; i++) {
                int compressedValue = columnReader.readBits();
                result.add(reversedMap.get(compressedValue));
            }
            return result;

        } catch (Exception e) {
            System.err.println("Uncompression failed " + e.getMessage());
            return new ArrayList<>();
        }
        
    }

    public List<Double> readAndUncompressRelevantDoubleData(String columnName, Map<String, List<Integer>> relevantZonesIndices) throws IOException {

        String compressedPath = columnStore.getDataDirectory() + java.io.File.separator + columnName + ".cmp";
        List<ZoneMetadata> columnZones = ZoneMetadata.readZoneMetadata(columnName, columnStore.getDataDirectory());

        List<Integer> compressedData = ZoneMetadata.readFilteredCompressedZones(Paths.get(compressedPath), columnZones, relevantZonesIndices.get("zones"), relevantZonesIndices.get("indices"), 800);

        try {
            List<Double> result = new ArrayList<>();

            Map<String, Integer> columnDict = loadDictionary(columnName);

            Map<Integer, String> reversedMap = reverseMap(columnDict);

            for (int index : compressedData) {
                result.add(Double.parseDouble(reversedMap.get(index)));
            }
            return result;
        } catch (Exception e) {
            System.err.println("Uncompression failed " + e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
     * Helper method to load compressed data
     */
    private byte[] loadCompressedData(String columnName) {
        try {
            String compressedPath = columnStore.getDataDirectory() + java.io.File.separator + columnName + ".cmp";
            return java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(compressedPath));
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Helper class to read bits from a byte array
     */
    public class BitStreamReader {
        private byte[] data;
        private int bitsPerValue;
        private int position = 0;
        private int bitPosition = 0;
        
        public BitStreamReader(byte[] data, int bitsPerValue) {
            this.data = data;
            this.bitsPerValue = bitsPerValue;
        }
        
        public void skipMetadata() {
            // Skip past the 8-byte header (bitsPerValue and valueCount as 4-byte ints)
            position = 8;
            bitPosition = 0;
        }
        
        public int readBits() {
            int result = 0;
            int bitsRead = 0;
            
            while (bitsRead < bitsPerValue) {
                int bitsAvailable = 8 - bitPosition;
                int bitsToRead = Math.min(bitsPerValue - bitsRead, bitsAvailable);
                
                // Extract bits from current byte
                int mask = ((1 << bitsToRead) - 1) << (bitsAvailable - bitsToRead);
                int extractedBits = (data[position] & mask) >>> (bitsAvailable - bitsToRead);
                
                // Add to result
                result = (result << bitsToRead) | extractedBits;
                
                // Update positions
                bitPosition += bitsToRead;
                bitsRead += bitsToRead;
                
                // Move to next byte if needed
                if (bitPosition == 8) {
                    position++;
                    bitPosition = 0;
                }
            }
            
            return result;
        }
    }

    public static Map<Integer, String> reverseMap(Map<String, Integer> originalMap) {
        Map<Integer, String> reversedMap = new HashMap<>();
        
        for (Map.Entry<String, Integer> entry : originalMap.entrySet()) {
            reversedMap.put(entry.getValue(), entry.getKey());
        }
        
        return reversedMap;
    }
    
    public List<Integer> readFilteredCompressedZones(Path columnCmpPath, List<ZoneMetadata> columnZones, List<Integer> filteredZones, int bitsPerValue, int valuesPerZone) throws IOException {
        List<Integer> relevantColumnData = new ArrayList<>();

        try (RandomAccessFile file = new RandomAccessFile(columnCmpPath.toFile(), "r")) {
            for (Integer zoneIndex : filteredZones) {
                ZoneMetadata zoneMetadata = columnZones.get(zoneIndex);

                long bytesToRead = zoneMetadata.getEndByte() - zoneMetadata.getStartByte();
                file.seek(zoneMetadata.getStartByte());

                byte[] dataBuffer = new byte[(int) bytesToRead];
                file.readFully(dataBuffer);

                // Decompress the buffer to get actual values
                List<Integer> values = CompressedColumnStore.readCompressedData(dataBuffer, bitsPerValue, valuesPerZone);

                relevantColumnData.addAll(values); // optionally filter values based on your predicate here
            }
        }

        return relevantColumnData;
    }

    /**
     * Query 1: Get minimum resale price for a specific month and town
     */
    public String getMinimumPrice(String yearMonth, String town) throws IOException {
        List<Integer> subset = getSubsetByMonthAndTownOptimized(yearMonth, town);
        if (subset.isEmpty()) {
            return "No result";
        }
        List<String> prices = readAndUncompressData("resale_price");
        Double minPrice = Double.MAX_VALUE;
        for (int i : subset) {
            minPrice = Math.min(minPrice, Double.parseDouble(prices.get(i)));
        }
        return String.valueOf(minPrice);
    }
    
    /**
     * Query 2: Calculate standard deviation of prices for a specific month and town
     */
    public String getStandardDeviationPrice(String yearMonth, String town) throws IOException {
        List<Integer> subset = getSubsetByMonthAndTownOptimized(yearMonth, town);
        if (subset.isEmpty()) {
            return "No result";
        }
        List<String> prices = readAndUncompressData("resale_price");
                
        double sum = 0.0;
        double variance = 0.0;

        for (int i : subset) {
            sum += Double.parseDouble(prices.get(i)); // To calculate mean
        }
        double mean = sum / subset.size();

        for (int i : subset) {
            variance += Math.pow(Double.parseDouble(prices.get(i)) - mean, 2);
        }
        variance /= (subset.size()-1);
        
        // Return standard deviation (square root of variance)
        return String.valueOf(Math.sqrt(variance));
    }
    
    /**
     * Query 3: Calculate average resale price for a specific month and town
     */
    public String getAveragePrice(String yearMonth, String town) throws IOException {
        List<Integer> subset = getSubsetByMonthAndTownOptimized(yearMonth, town);
        if (subset.isEmpty()) {
            return "No result";
        }

        List<String> prices = readAndUncompressData("resale_price");
                
        double sum = 0.0;
        for (int i : subset) {
            sum += Double.parseDouble(prices.get(i)); // To calculate mean
        }
        return String.valueOf(sum / subset.size());
    }
    
    /**
     * Query 4: Calculate minimum price per square meter for a specific month and town
     */
    public String getMinimumPricePerSquareMeter(String yearMonth, String town) throws IOException {
        List<Integer> subset = getSubsetByMonthAndTownOptimized(yearMonth, town);
        if (subset.isEmpty()) {
            return "No result";
        }

        List<String> prices = readAndUncompressData("resale_price");
        List<String> areas = readAndUncompressData("floor_area_sqm");
        
        double minPricePerSqm = Double.MAX_VALUE;
        for (int index : subset) {
            double price = Double.parseDouble(prices.get(index));
            double area = Double.parseDouble(areas.get(index));
            double pricePerSqm = price / area;
            
            minPricePerSqm = Math.min(minPricePerSqm, pricePerSqm);
        }
        
        return String.valueOf(minPricePerSqm);
    }
    
    /**
     * Run all queries for a specific month and town
     */
    public Map<String, Object> runAllQueries(String yearMonth, String town) throws IOException {
        Map<String, Object> resultsAndTimings = new HashMap<>();
        Map<String, String> results = new HashMap<>();
        resultsAndTimings.put("results", results);
        Map<String, Double> timings = new HashMap<>();
        resultsAndTimings.put("timings", timings);

        double totalTime = 0.0;
        
        // Get subset size
        TimerUtil.TimedResult<Integer> subset = TimerUtil.timeFunction(() -> getSubsetByMonthAndTownOptimized(yearMonth, town).size());
        results.put("Subset Size", String.valueOf(subset.getResult()));
        timings.put("Subset Size", subset.getDurationMs());
        totalTime += subset.getDurationMs();
        System.out.println("Time taken to filter on Compressed columns: " + String.valueOf(subset.getDurationMs()) + "ms");
        
        // Run all queries
        // Minimum Price
        TimerUtil.TimedResult<String> minPrice = TimerUtil.timeFunction(() -> getMinimumPrice(yearMonth, town));
        results.put("Minimum Price", minPrice.getResult());
        timings.put("Minimum Price", minPrice.getDurationMs());
        totalTime += minPrice.getDurationMs();

        // Standard Deviation of Price
        TimerUtil.TimedResult<String> stdDevPrice = TimerUtil.timeFunction(() -> getStandardDeviationPrice(yearMonth, town));
        results.put("Standard Deviation of Price", stdDevPrice.getResult());
        timings.put("Standard Deviation of Price", stdDevPrice.getDurationMs());
        totalTime += stdDevPrice.getDurationMs();

        // Average Price
        TimerUtil.TimedResult<String> avgPrice = TimerUtil.timeFunction(() -> getAveragePrice(yearMonth, town));
        results.put("Average Price", avgPrice.getResult());
        timings.put("Average Price", avgPrice.getDurationMs());
        totalTime += avgPrice.getDurationMs();

        // Minimum Price per Square Meter
        TimerUtil.TimedResult<String> minPsm = TimerUtil.timeFunction(() -> getMinimumPricePerSquareMeter(yearMonth, town));
        results.put("Minimum Price per Square Meter", minPsm.getResult());
        timings.put("Minimum Price per Square Meter", minPsm.getDurationMs());
        totalTime += minPsm.getDurationMs();

        // Print total time taken for all queries
        System.out.println("Total Time for all queries: " + totalTime + "ms");

        return resultsAndTimings;
    }

    public Map<String, List<Integer>> getRelevantZonesIndices(String yearMonth, String town) throws IOException {
        String nextMonthStr = PerformanceTest.getNextMonthStr(yearMonth);

        // Check if we can use the optimized path with compressed dictionaries
        Map<String, Integer> monthDict = loadDictionary("month");
        Map<String, Integer> townDict = loadDictionary("town");
        Map<String, Integer> floor_area_sqmDict = loadDictionary("floor_area_sqm");
        Map<Integer, String> reversedFloorAreaSqmDict = reverseMap(floor_area_sqmDict);
        
        // Get the indices for our target values
        Integer monthIndex1 = monthDict.getOrDefault(yearMonth, -1);
        Integer monthIndex2 = monthDict.getOrDefault(nextMonthStr, -1);
        Integer townIndex = townDict.get(town);

        Map<String, List<Integer>> relevantZonesIndices = ZoneMetadata.getCompressedZonesIndicesFromRelevantZones(monthIndex1, monthIndex2, townIndex, columnStore.getDataDirectory(), reversedFloorAreaSqmDict);

        return relevantZonesIndices;
    }

    public String getMinimumPriceZoneMap(String yearMonth, String town) throws IOException {
        Map<String, List<Integer>> relevantZonesIndices = getRelevantZonesIndices(yearMonth, town);

        if (relevantZonesIndices.get("indices").size() == 0) return "No result";

        List<Double> resalePrices = readAndUncompressRelevantDoubleData("resale_price", relevantZonesIndices);

        return String.valueOf(Collections.min(resalePrices));
    }

    public String getStandardDeviationPriceZoneMap(String yearMonth, String town) throws IOException {
        Map<String, List<Integer>> relevantZonesIndices = getRelevantZonesIndices(yearMonth, town);

        if (relevantZonesIndices.get("indices").size() == 0) return "No result";

        List<Double> resalePrices = readAndUncompressRelevantDoubleData("resale_price", relevantZonesIndices);

        double sum = 0.0;
        double variance = 0.0;

        for (double i : resalePrices) {
            sum += i; // To calculate mean
        }
        double mean = sum / resalePrices.size();

        for (double i : resalePrices) {
            variance += Math.pow(i - mean, 2);
        }
        variance /= (resalePrices.size()-1);
        
        // Return standard deviation (square root of variance)
        return String.valueOf(Math.sqrt(variance));
    }

        /**
     * Query 3: Calculate average resale price for a specific month and town
     */
    public String getAveragePriceZoneMap(String yearMonth, String town) throws IOException {
        Map<String, List<Integer>> relevantZonesIndices = getRelevantZonesIndices(yearMonth, town);

        if (relevantZonesIndices.get("indices").size() == 0) return "No result";

        List<Double> resalePrices = readAndUncompressRelevantDoubleData("resale_price", relevantZonesIndices);

        double sum = 0.0;
        for (double i : resalePrices) {
            sum += i; // To calculate mean
        }
        return String.valueOf(sum / resalePrices.size());
    }
    
    /**
     * Query 4: Calculate minimum price per square meter for a specific month and town
     */
    public String getMinimumPricePerSquareMeterZoneMap(String yearMonth, String town) throws IOException {
        Map<String, List<Integer>> relevantZonesIndices = getRelevantZonesIndices(yearMonth, town);

        if (relevantZonesIndices.get("indices").size() == 0) return "No result";

        List<Double> resalePrices = readAndUncompressRelevantDoubleData("resale_price", relevantZonesIndices);
        List<Double> areas = readAndUncompressRelevantDoubleData("floor_area_sqm", relevantZonesIndices);
        
        double minPricePerSqm = Double.MAX_VALUE;
        for (int index = 0; index < relevantZonesIndices.get("indices").size(); index++) {
            double price = resalePrices.get(index);
            double area = areas.get(index);
            double pricePerSqm = price / area;
            
            minPricePerSqm = Math.min(minPricePerSqm, pricePerSqm);
        }
        
        return String.valueOf(minPricePerSqm);
    }

        /**
     * Run all queries for a specific month and town
     */
    public Map<String, Object> runAllQueriesZoneMap(String yearMonth, String town) throws IOException {
        Map<String, Object> resultsAndTimings = new HashMap<>();
        Map<String, String> results = new HashMap<>();
        resultsAndTimings.put("results", results);
        Map<String, Double> timings = new HashMap<>();
        resultsAndTimings.put("timings", timings);

        double totalTime = 0.0;
        
        // Get subset size
        TimerUtil.TimedResult<Integer> subset = TimerUtil.timeFunction(() -> getRelevantZonesIndices(yearMonth, town).size());
        results.put("Subset Size", String.valueOf(subset.getResult()));
        timings.put("Subset Size", subset.getDurationMs());
        totalTime += subset.getDurationMs();
        System.out.println("Time taken to filter on Compressed columns: " + String.valueOf(subset.getDurationMs()) + "ms");
        
        // Run all queries
        // Minimum Price
        TimerUtil.TimedResult<String> minPrice = TimerUtil.timeFunction(() -> getMinimumPriceZoneMap(yearMonth, town));
        results.put("Minimum Price", minPrice.getResult());
        timings.put("Minimum Price", minPrice.getDurationMs());
        totalTime += minPrice.getDurationMs();

        // Standard Deviation of Price
        TimerUtil.TimedResult<String> stdDevPrice = TimerUtil.timeFunction(() -> getStandardDeviationPriceZoneMap(yearMonth, town));
        results.put("Standard Deviation of Price", stdDevPrice.getResult());
        timings.put("Standard Deviation of Price", stdDevPrice.getDurationMs());
        totalTime += stdDevPrice.getDurationMs();

        // Average Price
        TimerUtil.TimedResult<String> avgPrice = TimerUtil.timeFunction(() -> getAveragePriceZoneMap(yearMonth, town));
        results.put("Average Price", avgPrice.getResult());
        timings.put("Average Price", avgPrice.getDurationMs());
        totalTime += avgPrice.getDurationMs();

        // Minimum Price per Square Meter
        TimerUtil.TimedResult<String> minPsm = TimerUtil.timeFunction(() -> getMinimumPricePerSquareMeterZoneMap(yearMonth, town));
        results.put("Minimum Price per Square Meter", minPsm.getResult());
        timings.put("Minimum Price per Square Meter", minPsm.getDurationMs());
        totalTime += minPsm.getDurationMs();

        // Print total time taken for all queries
        System.out.println("Total Time for all queries: " + totalTime + "ms");

        return resultsAndTimings;
    }
}