// ColumnStore.java
import java.io.*;
import java.nio.file.*;
import java.util.*;

public class NormalColumnStore {
    private String dataDirectory;
    private List<String> columnNames;
    
    public NormalColumnStore(String dataDirectory) {
        this.dataDirectory = dataDirectory;
        this.columnNames = new ArrayList<>();
        
        // Create the data directory if it doesn't exist
        File dir = new File(dataDirectory);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    public String getDataDirectory() {
        return dataDirectory;
    }
    
    public void loadFromCSV(String csvFilePath) throws IOException {
        Path csvPath = Paths.get(csvFilePath);
        if (!Files.exists(csvPath)) {
            throw new IOException("CSV file does not exist: " + csvFilePath);
        }

        Map<String, List<ZoneMetadata>> columnMetadataList = new HashMap<>();
        Map<String, List<String>> columnValuesMap = new HashMap<>();
        Map<String, String> columnZoneMinMap = new HashMap<>();
        Map<String, String> columnZoneMaxMap = new HashMap<>();
        Map<String, Long> columnZoneStartByte = new HashMap<>();
        Map<String, Long> columnZoneEndByte = new HashMap<>();

        try (BufferedReader reader = Files.newBufferedReader(csvPath)) {
            // Read the header to get column names
            String header = reader.readLine();
            if (header == null) {
                throw new IOException("CSV file is empty");
            }

            String[] headers = header.split(",");
            columnNames = Arrays.asList(headers);

            // Initialize the metadata and column storage
            for (String columnName : columnNames) {
                columnValuesMap.put(columnName, new ArrayList<>());
                columnMetadataList.put(columnName, new ArrayList<>());
                columnZoneMinMap.put(columnName, null);
                columnZoneMaxMap.put(columnName, null);
                columnZoneStartByte.put(columnName, 0L);
                columnZoneEndByte.put(columnName, 0L);
            }

            // Process each row
            String line;
            int rowCount = 0;

            while ((line = reader.readLine()) != null) {
                rowCount++;
                String[] values = line.split(",");

                // Make sure we have the right number of values
                if (values.length != columnNames.size()) {
                    System.err.println("Warning: Row has incorrect number of values: " + line);
                    continue;
                }

                // Process each column in the row
                for (int i = 0; i < values.length; i++) {
                    String columnName = columnNames.get(i);
                    List<String> columnValues = columnValuesMap.get(columnName);

                    try {
                        String columnValue = values[i];
                        if (columnZoneMinMap.get(columnName) != null) {
                            if (columnZoneMinMap.get(columnName).compareTo(columnValue) > 0){
                                columnZoneMinMap.put(columnName, columnValue);
                            }
                        }
                        else {
                            columnZoneMinMap.put(columnName, columnValue);
                        }

                        if (columnZoneMaxMap.get(columnName) != null) {
                            if (columnZoneMaxMap.get(columnName).compareTo(columnValue) < 0){
                                columnZoneMaxMap.put(columnName, columnValue);
                            }
                        }
                        else {
                            columnZoneMaxMap.put(columnName, columnValue);
                        }

                    } catch (NumberFormatException e) {
                        // Handle non-numeric values gracefully, if required
                    }

                    // Add the value to the respective column's list of values
                    columnValues.add(values[i]);
                    columnZoneEndByte.put(columnName,  columnZoneEndByte.get(columnName) + values[i].length() + System.lineSeparator().length());
                }

                // If we've reached 800 values in the column, save the data
                if (rowCount % 800 == 0) {
                    for (String columnName : columnNames) {
                        List<ZoneMetadata> columnMetadata = columnMetadataList.get(columnName);
                        columnMetadata.add(new ZoneMetadata(columnZoneMaxMap.get(columnName), columnZoneMinMap.get(columnName), columnZoneStartByte.get(columnName), columnZoneEndByte.get(columnName)));
                        columnZoneMinMap.put(columnName, null);
                        columnZoneMaxMap.put(columnName, null);
                        columnZoneStartByte.put(columnName, columnZoneEndByte.get(columnName));
                        }
                }
            }

            // Handle the last remaining values for each column
            for (String columnName : columnNames) {
                List<String> columnValues = columnValuesMap.get(columnName);
                if (!columnValues.isEmpty()) {
                    saveColumnData(columnValues, columnName);
                    List<ZoneMetadata> columnMetadata = columnMetadataList.get(columnName);
                    columnMetadata.add(new ZoneMetadata(columnZoneMaxMap.get(columnName), columnZoneMinMap.get(columnName), columnZoneStartByte.get(columnName), columnZoneEndByte.get(columnName)));
                    columnZoneStartByte.put(columnName, columnZoneEndByte.get(columnName));
                }
            }

            // Save the column metadata to a separate file
            saveColumnMetadata(columnMetadataList);

        }
    }

    private void saveColumnData(List<String> columnValues, String columnName) throws IOException {
        Path columnFilePath = Paths.get(dataDirectory, columnName + ".col");
        try (BufferedWriter writer = Files.newBufferedWriter(columnFilePath)) {
            // Write each value for the column to its file
            for (String value : columnValues) {
                writer.write(value);
                writer.newLine();
            }
        }
    }

    private void saveColumnMetadata(Map<String, List<ZoneMetadata>> columnMetadataMap) throws IOException {
        for (Map.Entry<String, List<ZoneMetadata>> entry : columnMetadataMap.entrySet()) {
            String columnName = entry.getKey();
            Path metadataFile = Paths.get(dataDirectory, columnName + "_zone_map.txt");

            try (BufferedWriter metadataWriter = Files.newBufferedWriter(metadataFile)) {
                for (ZoneMetadata zone : entry.getValue()) {
                    metadataWriter.write(zone.toString());
                    metadataWriter.newLine();
                }
            }
        }
    }

    public List<String> readColumn(String columnName) throws IOException {
        List<String> columnValues = new ArrayList<>();
        String columnFilePath = dataDirectory + File.separator + columnName + ".col";
        
        // Open the BufferedReader for the specific column file
        try (BufferedReader reader = new BufferedReader(new FileReader(columnFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                columnValues.add(line);
            }
        }
        
        return columnValues;
    }
    
    public List<String> getColumnData(String columnName) throws IOException {
        if (!columnNames.contains(columnName)) {
            throw new IllegalArgumentException("Column not found: " + columnName);
        }
        
        String columnFilePath = dataDirectory + File.separator + columnName + ".col";
        return Files.readAllLines(Paths.get(columnFilePath));
    }
    
    public Map<String, String> getRow(int rowIndex) throws IOException {
        Map<String, String> row = new HashMap<>();
        
        for (String columnName : columnNames) {
            List<String> columnData = getColumnData(columnName);
            if (rowIndex >= 0 && rowIndex < columnData.size()) {
                row.put(columnName, columnData.get(rowIndex));
            } else {
                throw new IndexOutOfBoundsException("Row index out of bounds: " + rowIndex);
            }
        }
        
        return row;
    }
    
    public List<String> getColumnNames() {
        return columnNames;
    }
}