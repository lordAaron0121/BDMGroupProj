// ColumnStore.java
import java.io.*;
import java.nio.charset.StandardCharsets;
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

        Map<String, List<String>> columnValuesMap = new HashMap<>();

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
            }

            // Process each row
            String line;

            while ((line = reader.readLine()) != null) {
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

                    // Add the value to the respective column's list of values
                    columnValues.add(values[i]);
                }

            }

            // Handle the last remaining values for each column
            for (String columnName : columnNames) {
                List<String> columnValues = columnValuesMap.get(columnName);
                if (!columnValues.isEmpty()) {
                    saveColumnData(columnValues, columnName);
                }
            }

        }
    }

    public void generateZoneMapsFromColumns(int chunkSize) throws IOException {
        Map<String, List<ZoneMetadata>> columnZoneMaps = new HashMap<>();
        Map<String, Boolean> allDoublesList = new HashMap<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(dataDirectory), "*.col")) {
            for (Path columnFile : stream) {
                String columnName = columnFile.getFileName().toString().replace(".col", "");
                List<ZoneMetadata> zoneMetadataList = new ArrayList<>();
        
                boolean allDoubles = true;
        
                // First pass: determine if all lines are parsable as doubles
                try (BufferedReader reader = Files.newBufferedReader(columnFile)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (!isDouble(line)) {
                            allDoubles = false;
                            break;
                        }
                    }
                }
        
                // Second pass: process in chunks and collect zone metadata
                try (BufferedReader reader = Files.newBufferedReader(columnFile)) {
                    String line;
                    int lineCount = 0;
                    Object min = null, max = null;
                    long byteStart = 0;
                    long byteEnd = 0;
        
                    while ((line = reader.readLine()) != null) {
                        lineCount++;
                        long lineBytes = line.getBytes(StandardCharsets.UTF_8).length + System.lineSeparator().getBytes().length;
                        byteEnd += lineBytes;
        
                        if (allDoubles) {
                            Double value = Double.parseDouble(line);
                            if (min == null || ((Double) min) > value) min = value;
                            if (max == null || ((Double) max) < value) max = value;
                        } else {
                            if (min == null || min.toString().compareTo(line) > 0) min = line;
                            if (max == null || max.toString().compareTo(line) < 0) max = line;
                        }
        
                        if (lineCount % chunkSize == 0) {
                            if (allDoubles) {
                                zoneMetadataList.add(new ZoneMetadata((Double) min, (Double) max, byteStart, byteEnd));
                            } else {
                                zoneMetadataList.add(new ZoneMetadata(min.toString(), max.toString(), byteStart, byteEnd));
                            }
                            byteStart = byteEnd;
                            min = max = null;
                        }
                    }
        
                    // Final chunk
                    if (min != null && max != null) {
                        if (allDoubles) {
                            zoneMetadataList.add(new ZoneMetadata((Double) min, (Double) max, byteStart, byteEnd));
                        } else {
                            zoneMetadataList.add(new ZoneMetadata(min.toString(), max.toString(), byteStart, byteEnd));
                        }
                    }
        
                    columnZoneMaps.put(columnName, zoneMetadataList);
                    allDoublesList.put(columnName, allDoubles);
                }
            }
        }

        saveColumnMetadata(columnZoneMaps, allDoublesList);
    }

    // Utility method to check if a string is a valid Double
    private boolean isDouble(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
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

    private void saveColumnMetadata(Map<String, List<ZoneMetadata>> columnMetadataMap, Map<String, Boolean> allDoublesList) throws IOException {
        for (Map.Entry<String, List<ZoneMetadata>> entry : columnMetadataMap.entrySet()) {
            String columnName = entry.getKey();
            Path metadataFile = Paths.get(dataDirectory, columnName + "_zone_map.txt");

            try (BufferedWriter metadataWriter = Files.newBufferedWriter(metadataFile)) {
                metadataWriter.write(Boolean.toString(allDoublesList.get(columnName)));
                metadataWriter.newLine();
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