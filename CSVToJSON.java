import java.io.*;
import java.util.*;

public class CSVToJSON {
    public static void main(String[] args) throws Exception {
        // File paths
        String csvFilePath = "../data/ResalePricesSingapore.csv";
        String jsonFilePath = "output.json";

        // Step 1: Read the CSV file
        BufferedReader reader = new BufferedReader(new FileReader(csvFilePath));
        String line;

        // Read the header line (first line) to get the column names
        line = reader.readLine();
        String[] headers = line.split(",");

        // Step 2: Process data into column-based storage
        Map<String, List<String>> columnData = new LinkedHashMap<>();

        // Initialize lists for each column
        for (String header : headers) {
            columnData.put(header, new ArrayList<>());
        }

        // Step 3: Read each row and store the values in the corresponding columns
        while ((line = reader.readLine()) != null) {
            String[] values = line.split(",");
            for (int i = 0; i < headers.length; i++) {
                columnData.get(headers[i]).add(values[i]);
            }
        }

        reader.close();

        // Step 4: Convert the columnData map to a JSON-like string
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{\n");

        for (Map.Entry<String, List<String>> entry : columnData.entrySet()) {
            jsonBuilder.append("  \"").append(entry.getKey()).append("\": [\n");

            List<String> values = entry.getValue();
            for (int i = 0; i < values.size(); i++) {
                jsonBuilder.append("    \"").append(values.get(i)).append("\"");
                if (i < values.size() - 1) {
                    jsonBuilder.append(",");
                }
                jsonBuilder.append("\n");
            }

            jsonBuilder.append("  ]");
            if (entry != columnData.entrySet().toArray()[columnData.size() - 1]) {
                jsonBuilder.append(",");
            }
            jsonBuilder.append("\n");
        }

        jsonBuilder.append("}");

        // Step 5: Save the JSON-like string to a file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(jsonFilePath))) {
            writer.write(jsonBuilder.toString());
        }

        System.out.println("CSV data successfully converted to JSON.");
    }
}
