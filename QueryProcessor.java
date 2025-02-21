import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class QueryProcessor {
    public static List<Object> filter(List<Object> column, Predicate<Object> condition) {
        return column.stream().filter(condition).collect(Collectors.toList());
    }

    public static double min(List<Object> column) {
        return column.stream().mapToDouble(value -> Double.parseDouble((String) value)).min().orElse(Double.NaN);
    }

    public static double average(List<Object> column) {
        return column.stream().mapToDouble(value -> Double.parseDouble((String) value)).average().orElse(Double.NaN);
    }

    public static double standardDeviation(List<Object> column) {
        double mean = average(column);
        double variance = column.stream()
                .mapToDouble(value -> Double.parseDouble((String) value))
                .map(x -> Math.pow(x - mean, 2))
                .average().orElse(Double.NaN);
        return Math.sqrt(variance);
    }

    public static double minPricePerSquareMeter(List<Object> prices, List<Object> areas) {
        if (prices.size() != areas.size()) {
            throw new IllegalArgumentException("Price and area lists must have the same size");
        }
        double minPricePerSqm = Double.MAX_VALUE;
        for (int i = 0; i < prices.size(); i++) {
            double price = Double.parseDouble((String) prices.get(i));
            double area = Double.parseDouble((String) areas.get(i));
            double pricePerSqm = price / area;
            if (pricePerSqm < minPricePerSqm) {
                minPricePerSqm = pricePerSqm;
            }
        }
        return minPricePerSqm;
    }
}
