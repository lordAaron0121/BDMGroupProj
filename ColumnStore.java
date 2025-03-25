import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ColumnStore {
    private Map<String, List<Object>> columns;

    public ColumnStore() {
        this.columns = new HashMap<>();
    }

    public void addColumn(String columnName, List<Object> data) {
        columns.put(columnName, data);
    }

    public List<Object> getColumn(String columnName) {
        return columns.get(columnName);
    }

    public Set<String> getColumnNames() {
        return columns.keySet();
    }
}
