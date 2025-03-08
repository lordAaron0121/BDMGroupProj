import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryCache {
    private static final Map<String, Object> cache = new HashMap<>();
    
    public static void store(String key, Object value) {
        cache.put(key, value);
    }
    
    @SuppressWarnings("unchecked")
    public static <T> T get(String key) {
        return (T) cache.get(key);
    }
    
    public static boolean contains(String key) {
        return cache.containsKey(key);
    }
    
    public static void clear() {
        cache.clear();
    }
    
    public static String generateKey(String operation, String... params) {
        return String.join("_", operation, String.join("_", params));
    }
} 