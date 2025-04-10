import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.nio.file.Paths;

public class ResalePriceAnalyzer {
    public static void main(String[] args) throws IOException {
        String relativePath = "data/ResalePricesSingapore.csv";
        ColumnStore store = CSVLoader.loadCSV(relativePath);

        // Progressive filtering approach
        // 1. First filter by month (April and May 2016)
        List<Integer> monthFilteredIndices = new ArrayList<>();
        List<Object> months = store.getColumn("month");
        
        // First pass: Filter by month
        for (int i = 0; i < months.size(); i++) {
            String month = (String) months.get(i);
            if (month.equals("2016-04") || month.equals("2016-05")) {
                monthFilteredIndices.add(i);
            }
        }

        // 2. Then filter by town and floor area
        List<Integer> finalFilteredIndices = new ArrayList<>();
        List<Object> towns = store.getColumn("town");
        List<Object> floorAreas = store.getColumn("floor_area_sqm");

        for (int index : monthFilteredIndices) {
            try {
                if (towns.get(index).equals("CHOA CHU KANG") &&
                    Double.parseDouble((String) floorAreas.get(index)) >= 80) {
                    finalFilteredIndices.add(index);
                }
            } catch (NumberFormatException e) {
                System.err.println("Error parsing floor area: " + floorAreas.get(index) + " at index " + index);
            }
        }

        // Extract filtered data
        List<Object> prices = store.getColumn("resale_price");
        List<Object> filteredPrices = new ArrayList<>();
        List<Object> filteredAreas = new ArrayList<>();
        for (int index : finalFilteredIndices) {
            filteredPrices.add(prices.get(index));
            filteredAreas.add(floorAreas.get(index));
        }

        if (filteredPrices.isEmpty()) {
            System.out.println("No data found matching the criteria.");
            return;
        }

        // Calculate statistics
        double minPrice = QueryProcessor.min(filteredPrices);
        double avgPrice = QueryProcessor.average(filteredPrices);
        double stdDevPrice = QueryProcessor.standardDeviation(filteredPrices);
        double minPricePerSqm = QueryProcessor.minPricePerSquareMeter(filteredPrices, filteredAreas);

        // Print results
        System.out.printf("Minimum Price: %.2f\n", minPrice);
        System.out.printf("Average Price: %.2f\n", avgPrice);
        System.out.printf("Standard Deviation of Price: %.2f\n", stdDevPrice);
        System.out.printf("Minimum Price per Square Meter: %.2f\n", minPricePerSqm);
    }
}
