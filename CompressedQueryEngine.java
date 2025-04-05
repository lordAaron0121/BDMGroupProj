import java.io.IOException;
import java.util.*;

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
        
        // Calculate the next month for the range
        String[] parts = yearMonth.split("-");
        String year = parts[0];
        String month = parts[1];
        
        String nextMonthStr;
        if (month.equals("12")) {
            int nextYear = Integer.parseInt(year) + 1;
            nextMonthStr = nextYear + "-01";
        } else {
            int nextMonth = Integer.parseInt(month) + 1;
            nextMonthStr = year + "-" + (nextMonth < 10 ? "0" + nextMonth : String.valueOf(nextMonth));
        }
        
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
                    int recordCount = getRecordCount();
                    int monthBits = getBitsPerValue("month");
                    int townBits = getBitsPerValue("town");
                    int floor_area_sqmBits = getBitsPerValue("floor_area_sqm");
                    
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
                    if (line.startsWith("#")) continue;  // Skip comments
                    
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
    
    /**
     * Helper method to get the bits per value for a column
     */
    private int getBitsPerValue(String columnName) {
        try {
            String dictionaryPath = columnStore.getDataDirectory() + java.io.File.separator + columnName + ".dict";
            
            try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(dictionaryPath))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("# Bits used per value:")) {
                        return Integer.parseInt(line.split(":")[1].trim());
                    }
                }
            }
        } catch (Exception e) {
            // Ignore
        }
        
        return -1;  // Not found
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
     * Helper method to get record count from compressed file
     */
    private int getRecordCount() throws IOException {
        // Just use the first column to determine record count
        return columnStore.getColumnData(columnStore.getColumnNames().get(0)).size();
    }
    
    /**
     * Helper class to read bits from a byte array
     */
    private class BitStreamReader {
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
    
    /**
     * Query 1: Get minimum resale price for a specific month and town
     */
    public double getMinimumPrice(String yearMonth, String town) throws IOException {
        List<Integer> subset = getSubsetByMonthAndTownOptimized(yearMonth, town);
        if (subset.isEmpty()) {
            return 0.0;
        }

        Map<String, Integer> resale_priceDict = loadDictionary("resale_price");
        Map<Integer, String> reversedMap = reverseMap(resale_priceDict);

        byte[] resale_priceData = loadCompressedData("resale_price");
        int recordCount = getRecordCount();
        int resale_priceBits = getBitsPerValue("resale_price");
        BitStreamReader resale_priceReader = new BitStreamReader(resale_priceData, resale_priceBits);
        
        resale_priceReader.skipMetadata();
        Integer minPrice = Integer.MAX_VALUE;
        for (int i=0 ; i<recordCount ; i++) {
            int resale_priceIndex = resale_priceReader.readBits();
            if (subset.contains(i)) {
                minPrice = Math.min(minPrice, resale_priceIndex);
            }
        }
        return Double.parseDouble(reversedMap.get(minPrice));

        // List<String> prices = columnStore.getDecompressedColumnData("resale_price");
        // Double minPrice = Double.MAX_VALUE;
        // for (int i : subset) {
        //     minPrice = Math.min(minPrice, Double.parseDouble(prices.get(i)));
        // }
        // return minPrice;
    }
    
    /**
     * Query 2: Calculate standard deviation of prices for a specific month and town
     */
    public double getStandardDeviationPrice(String yearMonth, String town) throws IOException {
        List<Integer> subset = getSubsetByMonthAndTownOptimized(yearMonth, town);
        if (subset.isEmpty()) {
            return 0.0;
        }
        
        Map<String, Integer> resale_priceDict = loadDictionary("resale_price");
        Map<Integer, String> reversedMap = reverseMap(resale_priceDict);

        byte[] resale_priceData = loadCompressedData("resale_price");
        int recordCount = getRecordCount();
        int resale_priceBits = getBitsPerValue("resale_price");
        BitStreamReader resale_priceReader = new BitStreamReader(resale_priceData, resale_priceBits);
        
        resale_priceReader.skipMetadata();

        List<Double> prices_list = new ArrayList<>();
        double sum = 0.0;
        double variance = 0.0;
        for (int i = 0; i < recordCount; i++) {
            int resale_priceIndex = resale_priceReader.readBits();
            if (subset.contains(i)) {
                double resalePrice = Double.parseDouble(reversedMap.get(resale_priceIndex));
                sum += resalePrice; // To calculate mean
                prices_list.add(resalePrice);
            }
        }
        double mean = sum / subset.size();
        
        for (double price:prices_list) {
            variance += Math.pow(price - mean, 2);
        }
        variance /= (subset.size()-1);
        
        // Return standard deviation (square root of variance)
        return Math.sqrt(variance);
    }
    
    /**
     * Query 3: Calculate average resale price for a specific month and town
     */
    public double getAveragePrice(String yearMonth, String town) throws IOException {
        List<Integer> subset = getSubsetByMonthAndTownOptimized(yearMonth, town);
        if (subset.isEmpty()) {
            return 0.0;
        }

        Map<String, Integer> resale_priceDict = loadDictionary("resale_price");
        Map<Integer, String> reversedMap = reverseMap(resale_priceDict);

        byte[] resale_priceData = loadCompressedData("resale_price");
        int recordCount = getRecordCount();
        int resale_priceBits = getBitsPerValue("resale_price");
        BitStreamReader resale_priceReader = new BitStreamReader(resale_priceData, resale_priceBits);
        
        resale_priceReader.skipMetadata();
                
        double sum = 0.0;
        for (int i = 0; i < recordCount; i++) {
            int resale_priceIndex = resale_priceReader.readBits();
            if (subset.contains(i)) {
                double resalePrice = Double.parseDouble(reversedMap.get(resale_priceIndex));
                sum += resalePrice; // To calculate mean
            }
        }
        return sum / subset.size();        
    }
    
    /**
     * Query 4: Calculate minimum price per square meter for a specific month and town
     */
    public double getMinimumPricePerSquareMeter(String yearMonth, String town) throws IOException {
        List<Integer> subset = getSubsetByMonthAndTownOptimized(yearMonth, town);
        if (subset.isEmpty()) {
            return 0.0;
        }

        List<String> prices = columnStore.getDecompressedColumnData("resale_price");
        List<String> areas = columnStore.getDecompressedColumnData("floor_area_sqm");
        // List<String> prices = columnStore.getColumnData("resale_price");
        // List<String> areas = columnStore.getColumnData("floor_area_sqm");
        
        double minPricePerSqm = Double.MAX_VALUE;
        for (int index : subset) {
            double price = Double.parseDouble(prices.get(index));
            double area = Double.parseDouble(areas.get(index));
            double pricePerSqm = price / area;
            
            minPricePerSqm = Math.min(minPricePerSqm, pricePerSqm);
        }
        
        return minPricePerSqm;
    }
    
    /**
     * Run all queries for a specific month and town
     */
    public Map<String, Double> runAllQueries(String yearMonth, String town) throws IOException {
        Map<String, Double> results = new HashMap<>();
        
        // Get subset size
        int subsetSize = getSubsetByMonthAndTownOptimized(yearMonth, town).size();
        results.put("Subset Size", (double) subsetSize);
        
        // Run all queries
        results.put("Minimum Price", getMinimumPrice(yearMonth, town));
        results.put("Standard Deviation of Price", getStandardDeviationPrice(yearMonth, town));
        results.put("Average Price", getAveragePrice(yearMonth, town));
        results.put("Minimum Price per Square Meter", getMinimumPricePerSquareMeter(yearMonth, town));
        
        return results;
    }
}