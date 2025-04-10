import java.io.*;
import java.util.*;

public class ZMCSVLoader {
    public static ZMColumnStore loadCSV(String filePath, String saveDirectory) throws IOException {
        ZMColumnStore store = new ZMColumnStore();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String[] headers = reader.readLine().split(",");
            Map<String, List<Object>> columns = new HashMap<>();
            for (String header : headers) {
                columns.put(header, new ArrayList<>());
            }

            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                for (int i = 0; i < headers.length; i++) {
                    columns.get(headers[i]).add(parseValue(values[i], headers[i]));
                }
            }

            // Partition columns into zones and save them
            for (String columnName : columns.keySet()) {
                List<Object> columnData = columns.get(columnName);
                List<Zone> zones = partitionIntoZones(columnData);
                store.addZoneMap(columnName, zones);

                // Save each zone to disk
                for (int i = 0; i < zones.size(); i++) {
                    List<Object> zoneData = columnData.subList(zones.get(i).startIndex, zones.get(i).endIndex + 1);
                    store.saveZoneToFile(columnName, zoneData, i, saveDirectory);
                }
            }

            System.out.println("Columnar data saved to disk.");

            // Save the zone map to disk
            store.saveZoneMapToFile(store.getZoneMap(), saveDirectory);
        }
        return store;
    }

    // Helper method to parse values based on column type
    public static Object parseValue(String value, String columnName) {
        try {
            if (columnName.equals("Price")) {  // Example: column with numbers
                return Double.parseDouble(value);
            } else if (columnName.equals("Date")) {  // Example: column with dates
                return java.sql.Date.valueOf(value);
            } else {
                return value;  // Default to string
            }
        } catch (Exception e) {
            return value;  // If parsing fails, keep it as a string
        }
    }

// Method to partition each column into zones of 1000 values
public static List<Zone> partitionIntoZones(List<Object> columnData) {
    List<Zone> zones = new ArrayList<>();
    int numZones = (int) Math.ceil(columnData.size() / 1000.0);
    
    for (int i = 0; i < numZones; i++) {
        int startIndex = i * 1000;
        int endIndex = Math.min((i + 1) * 1000, columnData.size()) - 1;

        // Sublist for current zone data
        List<Object> zoneData = columnData.subList(startIndex, endIndex + 1);
        Object minValue = getMinValue(zoneData);
        Object maxValue = getMaxValue(zoneData);

        Zone zone = new Zone(minValue, maxValue, startIndex, endIndex);
        zones.add(zone);
    }
    return zones;
}

    // Method to calculate min value in a zone
    private static Object getMinValue(List<Object> data) {
        // Determine if the column data contains strings or numbers
        if (data.isEmpty()) return null;
        
        // Check if the first element is a number or string
        if (data.get(0) instanceof String) {
            return data.stream()
                    .map(Object::toString)
                    .min(String::compareTo)
                    .orElse(null);
        } else {
            return data.stream()
                    .mapToDouble(value -> value instanceof String ? Double.parseDouble((String) value) : ((Number) value).doubleValue())
                    .min()
                    .orElse(Double.NaN);
        }
    }

    // Method to calculate max value in a zone
    private static Object getMaxValue(List<Object> data) {
        // Determine if the column data contains strings or numbers
        if (data.isEmpty()) return null;
        
        // Check if the first element is a number or string
        if (data.get(0) instanceof String) {
            return data.stream()
                    .map(Object::toString)
                    .max(String::compareTo)
                    .orElse(null);
        } else {
            return data.stream()
                    .mapToDouble(value -> value instanceof String ? Double.parseDouble((String) value) : ((Number) value).doubleValue())
                    .max()
                    .orElse(Double.NaN);
        }
    }
}
