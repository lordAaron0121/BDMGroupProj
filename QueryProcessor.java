import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.ArrayList;

public class QueryProcessor {
    public static List<Object> filter(List<Object> column, Predicate<Object> condition, String cacheKey) {
        if (QueryCache.contains(cacheKey)) {
            return QueryCache.get(cacheKey);
        }
        
        List<Object> result = column.stream()
            .filter(condition)
            .collect(Collectors.toList());
            
        QueryCache.store(cacheKey, result);
        return result;
    }

    public static double min(List<Object> column) {
        String cacheKey = QueryCache.generateKey("min", column.toString());
        if (QueryCache.contains(cacheKey)) {
            return QueryCache.get(cacheKey);
        }
        
        double result = column.stream()
            .mapToDouble(value -> Double.parseDouble((String) value))
            .min()
            .orElse(Double.NaN);
            
        QueryCache.store(cacheKey, result);
        return result;
    }

    public static double average(List<Object> column) {
        String cacheKey = QueryCache.generateKey("avg", column.toString());
        if (QueryCache.contains(cacheKey)) {
            return QueryCache.get(cacheKey);
        }
        
        double result = column.stream()
            .mapToDouble(value -> Double.parseDouble((String) value))
            .average()
            .orElse(Double.NaN);
            
        QueryCache.store(cacheKey, result);
        return result;
    }

    public static double standardDeviation(List<Object> column) {
        String cacheKey = QueryCache.generateKey("stddev", column.toString());
        if (QueryCache.contains(cacheKey)) {
            return QueryCache.get(cacheKey);
        }
        
        double mean = average(column);
        double variance = column.stream()
            .mapToDouble(value -> Double.parseDouble((String) value))
            .map(x -> Math.pow(x - mean, 2))
            .average()
            .orElse(Double.NaN);
            
        double result = Math.sqrt(variance);
        QueryCache.store(cacheKey, result);
        return result;
    }

    public static double minPricePerSquareMeter(List<Object> prices, List<Object> areas) {
        String cacheKey = QueryCache.generateKey("minPricePerSqm", 
            prices.toString(), areas.toString());
            
        if (QueryCache.contains(cacheKey)) {
            return QueryCache.get(cacheKey);
        }
        
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
        
        QueryCache.store(cacheKey, minPricePerSqm);
        return minPricePerSqm;
    }
    
    // New method for performance testing
    public static long measureExecutionTime(Runnable task) {
        long startTime = System.nanoTime();
        task.run();
        long endTime = System.nanoTime();
        return (endTime - startTime) / 1_000_000; // Convert to milliseconds
    }
}
