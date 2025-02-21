import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class ResalePriceAnalyzer {
    public static void main(String[] args) throws IOException {
        ColumnStore store = CSVLoader.loadCSV("C:/Users/User/Downloads/Telegram Desktop/ResalePricesSingapore.csv");

        // Filter data for JURONG WEST in March 2021 with floor area >= 80 sq m
        List<Integer> filteredIndices = new ArrayList<>();
        List<Object> prices = store.getColumn("resale_price");
        List<Object> months = store.getColumn("month");
        List<Object> towns = store.getColumn("town");
        List<Object> floorAreas = store.getColumn("floor_area_sqm");

        for (int i = 0; i < prices.size(); i++) {
            try {
                if (months.get(i).equals("2021-01") &&
                        towns.get(i).equals("JURONG WEST") &&
                        Double.parseDouble((String) floorAreas.get(i)) >= 80) {
                    filteredIndices.add(i);
                }
            } catch (NumberFormatException e) {
                // Handle the exception (e.g., log it or skip this entry)
                System.err.println("Error parsing floor area: " + floorAreas.get(i) + " at index " + i);
            }
        }

        List<Object> filteredPrices = new ArrayList<>();
        List<Object> filteredAreas = new ArrayList<>();
        for (int index : filteredIndices) {
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
