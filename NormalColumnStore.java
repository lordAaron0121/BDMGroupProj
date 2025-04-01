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
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
            // Read the header to get column names
            String header = reader.readLine();
            if (header == null) {
                throw new IOException("CSV file is empty");
            }
            
            String[] headers = header.split(",");
            this.columnNames = Arrays.asList(headers);
            
            // Create file writers for each column
            Map<String, BufferedWriter> columnWriters = new HashMap<>();
            for (String columnName : columnNames) {
                String columnFilePath = dataDirectory + File.separator + columnName + ".col";
                columnWriters.put(columnName, new BufferedWriter(new FileWriter(columnFilePath)));
            }
            
            // Process each row and write values to corresponding column files
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                
                // Make sure we have the right number of values
                if (values.length != columnNames.size()) {
                    System.err.println("Warning: Row has incorrect number of values: " + line);
                    continue;
                }
                
                // Write each value to its column file
                for (int i = 0; i < values.length; i++) {
                    String columnName = columnNames.get(i);
                    BufferedWriter writer = columnWriters.get(columnName);
                    writer.write(values[i]);
                    writer.newLine();
                }
            }
            
            // Close all the writers
            for (BufferedWriter writer : columnWriters.values()) {
                writer.close();
            }
        }
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