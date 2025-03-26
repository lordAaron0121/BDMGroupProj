import java.io.*;
import java.util.*;
import java.util.function.Predicate;
import java.nio.file.*;

public class SplitRowStore {
    private final String baseDirectory;
    private final List<String> columnNames;
    private final int numFiles;
    private static final String FILE_EXTENSION = ".dat";

    public SplitRowStore(String baseDirectory) {
        this.baseDirectory = baseDirectory;
        this.columnNames = new ArrayList<>();
        this.numFiles = 4; // Split into 4 equal parts
        createBaseDirectory();
    }

    private void createBaseDirectory() {
        try {
            Files.createDirectories(Paths.get(baseDirectory));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create base directory: " + baseDirectory, e);
        }
    }

    public void saveData(List<Map<String, Object>> rows) {
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("Cannot save empty dataset");
        }

        // Get column names from first row
        columnNames.clear();
        columnNames.addAll(rows.get(0).keySet());

        // Calculate rows per file
        int rowsPerFile = (int) Math.ceil((double) rows.size() / numFiles);

        // Split and save data into files
        for (int i = 0; i < numFiles; i++) {
            int startIdx = i * rowsPerFile;
            int endIdx = Math.min(startIdx + rowsPerFile, rows.size());
            
            if (startIdx >= rows.size()) {
                break; // No more rows to process
            }

            List<Map<String, Object>> fileRows = rows.subList(startIdx, endIdx);
            String filePath = baseDirectory + File.separator + "part_" + i + FILE_EXTENSION;

            try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
                // Write column names first
                oos.writeObject(columnNames);
                // Write rows
                oos.writeObject(fileRows);
            } catch (IOException e) {
                throw new RuntimeException("Failed to save data to file: " + filePath, e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> readAllData() {
        List<Map<String, Object>> allRows = new ArrayList<>();
        
        for (int i = 0; i < numFiles; i++) {
            String filePath = baseDirectory + File.separator + "part_" + i + FILE_EXTENSION;
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
                // Read column names (we'll ignore these since we already have them)
                ois.readObject();
                // Read rows
                List<Map<String, Object>> fileRows = (List<Map<String, Object>>) ois.readObject();
                allRows.addAll(fileRows);
            } catch (IOException | ClassNotFoundException e) {
                // If file doesn't exist, we've reached the end of our data
                break;
            }
        }
        
        return allRows;
    }

    public List<Map<String, Object>> filter(Map<String, Predicate<Object>> conditions) {
        List<Map<String, Object>> results = new ArrayList<>();
        
        // Read and filter each file
        for (int i = 0; i < numFiles; i++) {
            String filePath = baseDirectory + File.separator + "part_" + i + FILE_EXTENSION;
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
                // Skip column names
                ois.readObject();
                // Read rows
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> fileRows = (List<Map<String, Object>>) ois.readObject();
                
                // Filter rows
                for (Map<String, Object> row : fileRows) {
                    boolean matchesAllConditions = true;
                    for (Map.Entry<String, Predicate<Object>> condition : conditions.entrySet()) {
                        if (!row.containsKey(condition.getKey()) || 
                            !condition.getValue().test(row.get(condition.getKey()))) {
                            matchesAllConditions = false;
                            break;
                        }
                    }
                    if (matchesAllConditions) {
                        results.add(row);
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                // If file doesn't exist, we've reached the end of our data
                break;
            }
        }
        
        return results;
    }

    public void deleteAllFiles() {
        for (int i = 0; i < numFiles; i++) {
            String filePath = baseDirectory + File.separator + "part_" + i + FILE_EXTENSION;
            try {
                Files.deleteIfExists(Paths.get(filePath));
            } catch (IOException e) {
                System.err.println("Failed to delete file: " + filePath);
            }
        }
        columnNames.clear();
    }

    public List<String> getColumnNames() {
        return new ArrayList<>(columnNames);
    }

    public int getNumFiles() {
        return numFiles;
    }
} 