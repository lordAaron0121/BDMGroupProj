import java.io.*;
import java.util.*;
import java.util.function.Predicate;
import java.nio.file.*;

public class RowStore {
    private List<Map<String, Object>> rows;
    private Set<String> columnNames;

    public RowStore() {
        this.rows = new ArrayList<>();
        this.columnNames = new HashSet<>();
    }

    public void addRow(Map<String, Object> row) {
        columnNames.addAll(row.keySet());
        rows.add(new HashMap<>(row));
    }

    public void addRows(List<Map<String, Object>> newRows) {
        for (Map<String, Object> row : newRows) {
            addRow(row);
        }
    }

    public List<Map<String, Object>> filter(Map<String, Predicate<Object>> conditions) {
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

    public List<Object> getColumn(String columnName) {
        if (!columnNames.contains(columnName)) {
            throw new IllegalArgumentException("Column not found: " + columnName);
        }

        List<Object> columnData = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            columnData.add(row.get(columnName));
        }
        return columnData;
    }

    public Set<String> getColumnNames() {
        return new HashSet<>(columnNames);
    }

    public int getRowCount() {
        return rows.size();
    }

    public void clear() {
        rows.clear();
        columnNames.clear();
    }
} 