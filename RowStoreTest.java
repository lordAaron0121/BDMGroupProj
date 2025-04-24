import java.util.*;
import java.util.function.Predicate;
import java.io.IOException;

public class RowStoreTest {
    public static void main(String[] args) throws IOException {
        // Initialize RowStore
        RowStore rowStore = new RowStore();
        
        // Define conditions
        Map<String, Predicate<Object>> conditions = new HashMap<>();
        conditions.put("month", month -> month.equals("2016-04") || month.equals("2016-05"));
        conditions.put("town", town -> town.equals("CHOA CHU KANG"));
        conditions.put("floor_area_sqm", area -> Double.parseDouble((String) area) >= 80);
        
        // Load data from CSV
        System.out.println("Loading data from CSV...");
        long startLoadTime = System.nanoTime();
        String filePath = "../data/ResalePricesSingapore.csv";
        rowStore = CSVLoader.rowStoreLoadCSV(filePath);
        long csvLoadTime = (System.nanoTime() - startLoadTime) / 1_000_000;
        System.out.println("CSV Loading Time: " + csvLoadTime + " ms");
        
        System.out.println("Total records: " + rowStore.getRowCount());
        
        // Minimum Price Query
        long startTime = System.nanoTime();
        List<Map<String, Object>> filteredRows = rowStore.filter(conditions);
        double minPrice = Double.MAX_VALUE;
        for (Map<String, Object> row : filteredRows) {
            minPrice = Math.min(minPrice, Double.parseDouble((String)row.get("resale_price")));
        }
        long minPriceTime = (System.nanoTime() - startTime) / 1_000_000;
        System.out.println("Minimum Price Query Time: " + minPriceTime + " ms");
        System.out.println("Minimum Price Total Time (including loading): " + (csvLoadTime + minPriceTime) + " ms");
        System.out.println("Minimum Price Result: $" + String.format("%.2f", minPrice));
        
        // Standard Deviation Query
        startTime = System.nanoTime();
        filteredRows = rowStore.filter(conditions);
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
        System.out.println("Standard Deviation Query Time: " + stdDevTime + " ms");
        System.out.println("Standard Deviation Total Time (including loading): " + (csvLoadTime + stdDevTime) + " ms");
        System.out.println("Standard Deviation Result: $" + String.format("%.2f", stdDev));
        
        // Average Price Query
        startTime = System.nanoTime();
        filteredRows = rowStore.filter(conditions);
        sum = 0.0;
        for (Map<String, Object> row : filteredRows) {
            sum += Double.parseDouble((String)row.get("resale_price"));
        }
        double avgPrice = sum / filteredRows.size();
        long avgPriceTime = (System.nanoTime() - startTime) / 1_000_000;
        System.out.println("Average Price Query Time: " + avgPriceTime + " ms");
        System.out.println("Average Price Total Time (including loading): " + (csvLoadTime + avgPriceTime) + " ms");
        System.out.println("Average Price Result: $" + String.format("%.2f", avgPrice));
        
        // Price per Square Meter Query
        startTime = System.nanoTime();
        filteredRows = rowStore.filter(conditions);
        double minPricePerSqm = Double.MAX_VALUE;
        for (Map<String, Object> row : filteredRows) {
            double price = Double.parseDouble((String)row.get("resale_price"));
            double area = Double.parseDouble((String)row.get("floor_area_sqm"));
            minPricePerSqm = Math.min(minPricePerSqm, price / area);
        }
        long pricePerSqmTime = (System.nanoTime() - startTime) / 1_000_000;
        System.out.println("Price per Square Meter Query Time: " + pricePerSqmTime + " ms");
        System.out.println("Price per Square Meter Total Time (including loading): " + (csvLoadTime + pricePerSqmTime) + " ms");
        System.out.println("Minimum Price per Square Meter Result: $" + String.format("%.2f", minPricePerSqm));
        
        System.out.println("Number of matching records: " + filteredRows.size());
        
        // Clear the store for next run
        rowStore.clear();
    }
} 