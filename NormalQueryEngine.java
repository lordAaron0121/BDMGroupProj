import java.io.IOException;
import java.util.*;

public class QueryEngine {
    private ColumnStore columnStore;
    
    public QueryEngine(ColumnStore columnStore) {
        this.columnStore = columnStore;
    }

    public List<Integer> getSubsetByMonthAndTown(String yearMonth, String town) throws IOException {
        List<String> months = columnStore.getColumnData("month");
        List<String> towns = columnStore.getColumnData("town");
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
            
            // Simple string matching: month equals yearMonth OR month equals nextMonthStr
            if ((monthValue.equals(yearMonth) || monthValue.equals(nextMonthStr)) && 
                townValue.equals(town)) {
                matchingIndices.add(i);
            }
        }
        
        System.out.println("Found " + matchingIndices.size() + " matching transactions");
        return matchingIndices;
    }
    
    /**
     * Query 1: Get minimum resale price for a specific month and town
     */
    public double getMinimumPrice(String yearMonth, String town) throws IOException {
        List<Integer> subset = getSubsetByMonthAndTown(yearMonth, town);
        if (subset.isEmpty()) {
            return 0.0;
        }
        
        List<String> prices = columnStore.getColumnData("resale_price");
        
        double minPrice = Double.MAX_VALUE;
        for (int index : subset) {
            double price = Double.parseDouble(prices.get(index));
            minPrice = Math.min(minPrice, price);
        }
        
        return minPrice;
    }
    
    /**
     * Query.2: Calculate standard deviation of prices for a specific month and town
     */
    public double getStandardDeviationPrice(String yearMonth, String town) throws IOException {
        List<Integer> subset = getSubsetByMonthAndTown(yearMonth, town);
        if (subset.isEmpty()) {
            return 0.0;
        }
        
        List<String> prices = columnStore.getColumnData("resale_price");
        
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
        variance /= subset.size();
        
        // Return standard deviation (square root of variance)
        return Math.sqrt(variance);
    }
    
    /**
     * Query 3: Calculate average resale price for a specific month and town
     */
    public double getAveragePrice(String yearMonth, String town) throws IOException {
        List<Integer> subset = getSubsetByMonthAndTown(yearMonth, town);
        if (subset.isEmpty()) {
            return 0.0;
        }
        
        List<String> prices = columnStore.getColumnData("resale_price");
        
        double sum = 0.0;
        for (int index : subset) {
            sum += Double.parseDouble(prices.get(index));
        }
        
        return sum / subset.size();
    }
    
    /**
     * Query 4: Calculate minimum price per square meter for a specific month and town
     */
    public double getMinimumPricePerSquareMeter(String yearMonth, String town) throws IOException {
        List<Integer> subset = getSubsetByMonthAndTown(yearMonth, town);
        if (subset.isEmpty()) {
            return 0.0;
        }
        
        List<String> prices = columnStore.getColumnData("resale_price");
        List<String> areas = columnStore.getColumnData("floor_area_sqm");
        
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
        int subsetSize = getSubsetByMonthAndTown(yearMonth, town).size();
        results.put("Subset Size", (double) subsetSize);
        
        // Run all queries
        results.put("Minimum Price", getMinimumPrice(yearMonth, town));
        results.put("Standard Deviation of Price", getStandardDeviationPrice(yearMonth, town));
        results.put("Average Price", getAveragePrice(yearMonth, town));
        results.put("Minimum Price per Square Meter", getMinimumPricePerSquareMeter(yearMonth, town));
        
        return results;
    }
}