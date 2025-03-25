import java.io.*;
import java.util.*;
import java.util.function.Predicate;
import java.nio.file.*;

public class SplitRowStore {
    private final String baseDirectory;
    private final Map<String, List<String>> fileGroups; // Maps group name to list of column names
    private final Map<String, Integer> rowCounts; // Maps group name to number of rows
    private static final String FILE_EXTENSION = ".dat";

    public SplitRowStore(String baseDirectory) {
        this.baseDirectory = baseDirectory;
        this.fileGroups = new HashMap<>();
        this.rowCounts = new HashMap<>();
        createBaseDirectory();
    }

    private void createBaseDirectory() {
        try {
            Files.createDirectories(Paths.get(baseDirectory));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create base directory: " + baseDirectory, e);
        }
    }

    public void createGroup(String groupName, List<String> columnNames) {
        fileGroups.put(groupName, columnNames);
    }

    public void saveData(String groupName, List<Map<String, Object>> rows) {
        if (!fileGroups.containsKey(groupName)) {
            throw new IllegalArgumentException("Group not found: " + groupName);
        }

        List<String> columnNames = fileGroups.get(groupName);
        String filePath = baseDirectory + File.separator + groupName + FILE_EXTENSION;
        rowCounts.put(groupName, rows.size());

        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
            // Write column names first
            oos.writeObject(columnNames);
            
            // Write rows with only the specified columns, plus record ID
            List<Map<String, Object>> filteredRows = new ArrayList<>();
            for (int i = 0; i < rows.size(); i++) {
                Map<String, Object> row = rows.get(i);
                Map<String, Object> filteredRow = new HashMap<>();
                filteredRow.put("_id", i); // Add record ID
                for (String column : columnNames) {
                    filteredRow.put(column, row.get(column));
                }
                filteredRows.add(filteredRow);
            }
            oos.writeObject(filteredRows);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save data for group: " + groupName, e);
        }
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> readGroup(String groupName) {
        if (!fileGroups.containsKey(groupName)) {
            throw new IllegalArgumentException("Group not found: " + groupName);
        }

        String filePath = baseDirectory + File.separator + groupName + FILE_EXTENSION;
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
            // Read column names
            List<String> columnNames = (List<String>) ois.readObject();
            // Read rows
            return (List<Map<String, Object>>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to read data for group: " + groupName, e);
        }
    }

    public List<Map<String, Object>> filter(String groupName, Map<String, Predicate<Object>> conditions) {
        List<Map<String, Object>> rows = readGroup(groupName);
        List<Map<String, Object>> filteredRows = new ArrayList<>();
        
        rowLoop:
        for (Map<String, Object> row : rows) {
            for (Map.Entry<String, Predicate<Object>> condition : conditions.entrySet()) {
                String columnName = condition.getKey();
                Predicate<Object> predicate = condition.getValue();
                
                if (!row.containsKey(columnName) || !predicate.test(row.get(columnName))) {
                    continue rowLoop;
                }
            }
            filteredRows.add(row);
        }
        
        return filteredRows;
    }

    public void deleteAllFiles() {
        for (String groupName : fileGroups.keySet()) {
            String filePath = baseDirectory + File.separator + groupName + FILE_EXTENSION;
            try {
                Files.deleteIfExists(Paths.get(filePath));
            } catch (IOException e) {
                System.err.println("Failed to delete file: " + filePath);
            }
        }
        fileGroups.clear();
        rowCounts.clear();
    }

    public Set<String> getGroups() {
        return new HashSet<>(fileGroups.keySet());
    }

    public List<String> getColumnsInGroup(String groupName) {
        return fileGroups.getOrDefault(groupName, new ArrayList<>());
    }

    public int getRowCount(String groupName) {
        return rowCounts.getOrDefault(groupName, 0);
    }
} 