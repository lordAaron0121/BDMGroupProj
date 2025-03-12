import java.io.*;
import java.util.*;
import java.util.function.Predicate;
import java.nio.file.*;

public class FileColumnStore {
    private final String baseDirectory;
    private final Map<String, String> columnFileMap;
    private static final String FILE_EXTENSION = ".col";

    public FileColumnStore(String baseDirectory) {
        this.baseDirectory = baseDirectory;
        this.columnFileMap = new HashMap<>();
        createBaseDirectory();
    }

    private void createBaseDirectory() {
        try {
            Files.createDirectories(Paths.get(baseDirectory));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create base directory: " + baseDirectory, e);
        }
    }

    public void saveColumn(String columnName, List<Object> data) {
        String filePath = baseDirectory + File.separator + columnName + FILE_EXTENSION;
        columnFileMap.put(columnName, filePath);

        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
            oos.writeObject(data);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save column: " + columnName, e);
        }
    }

    @SuppressWarnings("unchecked")
    public List<Object> readColumn(String columnName) {
        String filePath = columnFileMap.get(columnName);
        if (filePath == null) {
            throw new IllegalArgumentException("Column not found: " + columnName);
        }

        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
            return (List<Object>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to read column: " + columnName, e);
        }
    }

    public List<Integer> filterColumn(String columnName, Predicate<Object> condition) {
        List<Object> columnData = readColumn(columnName);
        List<Integer> qualifiedIndexes = new ArrayList<>();
        
        for (int i = 0; i < columnData.size(); i++) {
            if (condition.test(columnData.get(i))) {
                qualifiedIndexes.add(i);
            }
        }
        
        return qualifiedIndexes;
    }

    public List<Integer> sequentialFilter(List<String> columnNames, List<Predicate<Object>> conditions) {
        if (columnNames.size() != conditions.size()) {
            throw new IllegalArgumentException("Number of columns and conditions must match");
        }

        if (columnNames.isEmpty()) {
            return new ArrayList<>();
        }

        // Start with first column's filter results
        List<Integer> qualifiedIndexes = filterColumn(columnNames.get(0), conditions.get(0));

        // Sequentially filter remaining columns using only qualified indexes
        for (int i = 1; i < columnNames.size() && !qualifiedIndexes.isEmpty(); i++) {
            List<Object> columnData = readColumn(columnNames.get(i));
            List<Integer> newQualifiedIndexes = new ArrayList<>();

            for (Integer index : qualifiedIndexes) {
                if (index < columnData.size() && conditions.get(i).test(columnData.get(index))) {
                    newQualifiedIndexes.add(index);
                }
            }

            qualifiedIndexes = newQualifiedIndexes;
        }

        return qualifiedIndexes;
    }

    public void deleteAllFiles() {
        for (String filePath : columnFileMap.values()) {
            try {
                Files.deleteIfExists(Paths.get(filePath));
            } catch (IOException e) {
                System.err.println("Failed to delete file: " + filePath);
            }
        }
        columnFileMap.clear();
    }
} 