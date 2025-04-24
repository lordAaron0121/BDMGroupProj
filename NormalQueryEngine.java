import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;

public class NormalQueryEngine {
    private NormalColumnStore columnStore;
    
    public NormalQueryEngine(NormalColumnStore columnStore) {
        this.columnStore = columnStore;
    }

        public static void saveToCSV(Map<String, String> results, String year, String month, String town, String filePath) throws IOException {
        // Create a FileWriter to write to the CSV file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            // Write the header row
            writer.write("Year,Month,Town,Category,Value");
            writer.newLine(); // Newline after the header row

            List<String> categories = new ArrayList<>();
            categories.add("Minimum Price");
            categories.add("Standard Deviation of Price");
            categories.add("Average Price");
            categories.add("Minimum Price per Square Meter");

            // Write the data rows
            for (Map.Entry<String, String> entry : results.entrySet()) {
                String category = entry.getKey();
                if (categories.contains(category)) {
                    String value = entry.getValue();

                    if (value.equals("No result")) {
                        writer.write(String.format("%s,%s,%s,%s,%s", year, month, town, category, value));
                        writer.newLine();
                    }
                    
                    else {
                        // Write each row with year, month, town, category, and value
                        writer.write(String.format("%s,%s,%s,%s,%.2f", year, month, town, category, Double.parseDouble(value)));
                        writer.newLine(); // Newline after each entry
                    }
                }
            }
        }
    }

    public List<Integer> getSubsetByMonthAndTown(String yearMonth, String town) throws IOException {
        List<String> months = columnStore.readColumn("month");
        List<String> towns = columnStore.readColumn("town");
        List<String> floor_area_sqm = columnStore.readColumn("floor_area_sqm");

        List<Integer> matchingIndices = new ArrayList<>();
        
        // Simple calculation for next month
        String nextMonthStr = CompressionTestMain.getNextMonthStr(yearMonth);
        
        System.out.println("Filtering transactions with month = " + yearMonth + " OR month = " + nextMonthStr + " for town: " + town);
        
        // Find all rows that match the criteria
        for (int i = 0; i < months.size(); i++) {
            String monthValue = months.get(i);
            String townValue = towns.get(i);
            Double floorValue = Double.parseDouble(floor_area_sqm.get(i));
            
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
     * Query 1: Get minimum resale price for a specific month and town
     */
    public String getMinimumPrice(String yearMonth, String town) throws IOException {
        List<Integer> subset = getSubsetByMonthAndTown(yearMonth, town);
        if (subset.isEmpty()) {
            return "No result";
        }
        
        List<String> prices = columnStore.readColumn("resale_price");
        // List<String> prices = columnStore.getColumnData("resale_price");
        
        double minPrice = Double.MAX_VALUE;
        for (int index : subset) {
            double price = Double.parseDouble(prices.get(index));
            minPrice = Math.min(minPrice, price);
        }
        
        return String.valueOf(minPrice);
    }
    
    /**
     * Query.2: Calculate standard deviation of prices for a specific month and town
     */
    public String getStandardDeviationPrice(String yearMonth, String town) throws IOException {
        List<Integer> subset = getSubsetByMonthAndTown(yearMonth, town);
        if (subset.isEmpty()) {
            return "No result";
        }

        List<String> prices = columnStore.readColumn("resale_price");
        // List<String> prices = columnStore.getColumnData("resale_price");
        
        // Calculate mean first
        double sum = 0.0;
        for (int index : subset) {
            sum += Double.parseDouble(prices.get(index));
        }
        double mean = sum / subset.size();
        
        // Calculate variance
        double variance = 0.0;
        for (int index : subset) {
            double price = Double.parseDouble(prices.get(index));
            variance += Math.pow(price - mean, 2);
        }
        variance /= (subset.size() - 1);
        
        // Return standard deviation (square root of variance)
        return String.valueOf(Math.sqrt(variance));
    }
    
    /**
     * Query 3: Calculate average resale price for a specific month and town
     */
    public String getAveragePrice(String yearMonth, String town) throws IOException {
        List<Integer> subset = getSubsetByMonthAndTown(yearMonth, town);
        if (subset.isEmpty()) {
            return "No result";
        }
        
        List<String> prices = columnStore.readColumn("resale_price");
        // List<String> prices = columnStore.getColumnData("resale_price");
        
        double sum = 0.0;
        for (int index : subset) {
            sum += Double.parseDouble(prices.get(index));
        }
        
        return String.valueOf(sum / subset.size());
    }
    
    /**
     * Query 4: Calculate minimum price per square meter for a specific month and town
     */
    public String getMinimumPricePerSquareMeter(String yearMonth, String town) throws IOException {
        List<Integer> subset = getSubsetByMonthAndTown(yearMonth, town);
        if (subset.isEmpty()) {
            return "No result";
        }
        
        List<String> prices = columnStore.readColumn("resale_price");
        List<String> areas = columnStore.readColumn("floor_area_sqm");
        // List<String> prices = columnStore.getColumnData("resale_price");
        // List<String> areas = columnStore.getColumnData("floor_area_sqm");
        
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
        
        // Subset Size
        TimerUtil.TimedResult<Integer> subset = TimerUtil.timeFunction(() -> getSubsetByMonthAndTown(yearMonth, town).size());
        results.put("Subset Size", String.valueOf(subset.getResult()));
        timings.put("Subset Size", subset.getDurationMs());
        totalTime += subset.getDurationMs();
        System.out.println("Time taken to filter on Normal columns: " + String.valueOf(subset.getDurationMs()) + "ms");

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

        saveToCSV(results, yearMonth.split("-")[0], yearMonth.split("-")[1], town, "ScanResult_U2121346H.csv");

        // Print total time taken for all queries
        System.out.println("Total Time for all queries: " + totalTime + "ms");

        return resultsAndTimings;
    }

    public String getMinimumPriceZoneMap(String yearMonth, String town) throws IOException {
        Map<String, List<Integer>> relevantData = ZoneMetadata.getDataFromRelevantZones(yearMonth, town, columnStore.getDataDirectory());

        if (relevantData.get("indices").size() == 0) return "No result";

        List<Double> resalePrices = ZoneMetadata.readDoubleColumnDataInZones("resale_price", relevantData.get("zones"), relevantData.get("indices"), columnStore.getDataDirectory());

        return String.valueOf(Collections.min(resalePrices));
    }

    public String getStandardDeviationPriceZoneMap(String yearMonth, String town) throws IOException {
        Map<String, List<Integer>> relevantData = ZoneMetadata.getDataFromRelevantZones(yearMonth, town, columnStore.getDataDirectory());

        if (relevantData.get("indices").size() == 0) return "No result";

        List<Double> resalePrices = ZoneMetadata.readDoubleColumnDataInZones("resale_price", relevantData.get("zones"), relevantData.get("indices"), columnStore.getDataDirectory());

        double mean = resalePrices.stream()
            .mapToDouble(Double::doubleValue)
            .average()
            .orElse(0.0); // fallback if list is empty
        
        // Calculate variance
        double variance = 0.0;
        for (double resalePrice : resalePrices) {
            variance += Math.pow(resalePrice - mean, 2);
        }
        variance /= (resalePrices.size() - 1);
        
        // Return standard deviation (square root of variance)
        return String.valueOf(Math.sqrt(variance));
    }

    public String getAveragePriceZoneMap(String yearMonth, String town) throws IOException {
        Map<String, List<Integer>> relevantData = ZoneMetadata.getDataFromRelevantZones(yearMonth, town, columnStore.getDataDirectory());

        if (relevantData.get("indices").size() == 0) return "No result";

        List<Double> resalePrices = ZoneMetadata.readDoubleColumnDataInZones("resale_price", relevantData.get("zones"), relevantData.get("indices"), columnStore.getDataDirectory());
        
        double mean = resalePrices.stream()
            .mapToDouble(Double::doubleValue)
            .average()
            .orElse(0.0); // fallback if list is empty

        return String.valueOf(mean);
    }

    public String getMinimumPricePerSquareMeterZoneMap(String yearMonth, String town) throws IOException {
        Map<String, List<Integer>> relevantData = ZoneMetadata.getDataFromRelevantZones(yearMonth, town, columnStore.getDataDirectory());

        if (relevantData.get("indices").size() == 0) return "No result";

        List<Double> resalePrices = ZoneMetadata.readDoubleColumnDataInZones("resale_price", relevantData.get("zones"), relevantData.get("indices"), columnStore.getDataDirectory());
        List<Double> floorAreaSqm = ZoneMetadata.readDoubleColumnDataInZones("floor_area_sqm", relevantData.get("zones"), relevantData.get("indices"), columnStore.getDataDirectory());
        
        double minPricePerSqm = Double.MAX_VALUE;
        for (int index = 0 ; index < resalePrices.size(); index++) {
            double price = resalePrices.get(index);
            double area = floorAreaSqm.get(index);
            double pricePerSqm = price / area;
            
            minPricePerSqm = Math.min(minPricePerSqm, pricePerSqm);
        }
        
        return String.valueOf(minPricePerSqm);
    }

    public Map<String, Object> runAllQueriesZoneMap(String yearMonth, String town) throws IOException {
        Map<String, Object> resultsAndTimings = new HashMap<>();
        Map<String, String> results = new HashMap<>();
        resultsAndTimings.put("results", results);
        Map<String, Double> timings = new HashMap<>();
        resultsAndTimings.put("timings", timings);

        double totalTime = 0.0;

        // Get subset size
        TimerUtil.TimedResult<Integer> subset = TimerUtil.timeFunction(() -> ZoneMetadata.getDataFromRelevantZones(yearMonth, town, columnStore.getDataDirectory()).get("indices").size());
        results.put("Subset Size", String.valueOf(subset.getResult()));
        timings.put("Subset Size", subset.getDurationMs());
        totalTime += subset.getDurationMs();
        System.out.println("Time taken to filter on Normal columns with Zone Map: " + String.valueOf(subset.getDurationMs()) + "ms");
        
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