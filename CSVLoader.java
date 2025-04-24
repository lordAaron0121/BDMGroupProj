import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CSVLoader {

    public static RowStore rowStoreLoadCSV(String filePath) throws IOException {
        RowStore store = new RowStore();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String[] headers = reader.readLine().split(",");
            Map<String, List<Object>> columns = new HashMap<>();
            for (String header : headers) {
                columns.put(header, new ArrayList<>());
            }

            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                Map<String, Object> row = new HashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    row.put(headers[i], values[i]);
                }
                store.addRow(row);
            }
        }
        return store;
    }
}
