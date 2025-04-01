import java.io.*;
import java.nio.file.*;
import java.util.*;

public class CompressedColumnStore {
    private String dataDirectory;
    private List<String> columnNames;
    private Map<String, Boolean> isCompressed;
    
    public CompressedColumnStore(String dataDirectory) {
        this.dataDirectory = dataDirectory;
        this.columnNames = new ArrayList<>();
        this.isCompressed = new HashMap<>();
        
        // Create the data directory if it doesn't exist
        File dir = new File(dataDirectory);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }
    
    public String getDataDirectory() {
        return dataDirectory;
    }
    
    /**
     * Load data directly from a CSV file into the compressed column store
     */
    public void loadFromCSV(String csvFilePath) throws IOException {
        // Step 1: Read CSV and collect all column data
        Map<String, List<String>> allColumnData = readCSVIntoColumns(csvFilePath);
        
        // Step 2: Process and store each column (with or without compression)
        processAndStoreColumns(allColumnData);
        
        // Step 3: Save metadata about compression
        saveMetadata();
    }
    
    /**
     * Reads CSV file and organizes data by columns
     */
    private Map<String, List<String>> readCSVIntoColumns(String csvFilePath) throws IOException {
        Map<String, List<String>> allColumnData = new HashMap<>();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
            // Read the header to get column names
            String header = reader.readLine();
            if (header == null) {
                throw new IOException("CSV file is empty");
            }
            
            String[] headers = header.split(",");
            this.columnNames = Arrays.asList(headers);
            
            // Initialize column data lists
            for (String columnName : columnNames) {
                allColumnData.put(columnName, new ArrayList<>());
            }
            
            // Process each row and collect values
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                
                // Make sure we have the right number of values
                if (values.length != columnNames.size()) {
                    System.err.println("Warning: Row has incorrect number of values: " + line);
                    continue;
                }
                
                // Add each value to its column's list
                for (int i = 0; i < values.length; i++) {
                    String columnName = columnNames.get(i);
                    allColumnData.get(columnName).add(values[i]);
                }
            }
        }
        
        return allColumnData;
    }
    
    /**
     * Process each column - either compress it or store it directly
     */
    private void processAndStoreColumns(Map<String, List<String>> allColumnData) throws IOException {
        for (String columnName : columnNames) {
            List<String> columnData = allColumnData.get(columnName);
            
            // Evaluate if this column should be compressed
            boolean shouldCompress = evaluateForCompression(columnData);
            
            if (shouldCompress) {
                compressAndStoreColumn(columnName, columnData);
                isCompressed.put(columnName, true);
            } else {
                storeUncompressedColumn(columnName, columnData);
                isCompressed.put(columnName, false);
            }
        }
    }
    
    /**
     * Evaluate if a column should be compressed based on cardinality
     */
    private boolean evaluateForCompression(List<String> columnData) {
        // Get unique values for this column
        Set<String> uniqueValues = new HashSet<>(columnData);
        
        // Compress if less than 10% of values are unique and there's more than one unique value
        return (double)uniqueValues.size() / (double)columnData.size() < 0.1 && uniqueValues.size() > 1;
    }
    
    /**
     * Store column data without compression
     */
    private void storeUncompressedColumn(String columnName, List<String> columnData) throws IOException {
        String columnFilePath = dataDirectory + File.separator + columnName + ".col";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(columnFilePath))) {
            for (String value : columnData) {
                writer.write(value);
                writer.newLine();
            }
        }
    }
    
    /**
     * Compress and store column data
     */
    private void compressAndStoreColumn(String columnName, List<String> columnData) throws IOException {
        // Get unique values and create dictionary
        Set<String> uniqueValues = new HashSet<>(columnData);
        compressColumn(columnName, columnData, uniqueValues);
    }
    
    /**
     * Legacy method for backward compatibility - no longer depends on ColumnStore
     */
    public void loadFromColumnStore(String csvFilePath) throws IOException {
        // Simply redirect to the new method
        loadFromCSV(csvFilePath);
    }
    
    private void compressColumn(String columnName, List<String> columnData, Set<String> uniqueValues) 
        throws IOException {
        // Calculate bits needed
        int uniqueCount = uniqueValues.size();
        int bitsNeeded = (int) Math.ceil(Math.log(uniqueCount) / Math.log(2));
        
        // Create dictionary and save it
        Map<String, Integer> dictionary = createAndSaveDictionary(columnName, uniqueValues, bitsNeeded);
        
        // Convert and save column data in compressed format
        writeCompressedColumnData(columnName, columnData, dictionary, bitsNeeded);
    }
    
    /**
     * Create dictionary mapping values to indices and save it to file
     */
    private Map<String, Integer> createAndSaveDictionary(String columnName, Set<String> uniqueValues, int bitsNeeded) 
        throws IOException {
        // Create a sorted list of unique values
        List<String> sortedUniqueValues = new ArrayList<>(uniqueValues);
        Collections.sort(sortedUniqueValues);
        
        // Create dictionary: value -> index
        Map<String, Integer> dictionary = new HashMap<>();
        for (int i = 0; i < sortedUniqueValues.size(); i++) {
            dictionary.put(sortedUniqueValues.get(i), i);
        }
        
        // Save the dictionary to file
        String dictionaryPath = dataDirectory + File.separator + columnName + ".dict";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(dictionaryPath))) {
            writer.write("# Dictionary for column: " + columnName);
            writer.newLine();
            writer.write("# Format: value,index");
            writer.newLine();
            writer.write("# Bits used per value: " + bitsNeeded);
            writer.newLine();
            
            for (Map.Entry<String, Integer> entry : dictionary.entrySet()) {
                writer.write(entry.getKey() + "," + entry.getValue());
                writer.newLine();
            }
        }
        
        return dictionary;
    }
    
    /**
     * Write compressed column data using the dictionary
     */
    private void writeCompressedColumnData(String columnName, List<String> columnData, 
                                           Map<String, Integer> dictionary, int bitsNeeded) 
        throws IOException {
        BitOutputStream bos = null;
        try {
            String compressedFilePath = dataDirectory + File.separator + columnName + ".cmp";
            bos = new BitOutputStream(new FileOutputStream(compressedFilePath));
            
            // Write the number of bits per value and number of values as metadata
            bos.writeInt(bitsNeeded);
            bos.writeInt(columnData.size());
            
            // Write each value's index using the calculated number of bits
            for (String value : columnData) {
                int index = dictionary.get(value);
                bos.writeBits(index, bitsNeeded);
            }
        } finally {
            if (bos != null) {
                bos.close();
            }
        }
    }
    
    private void saveMetadata() throws IOException {
        String metadataPath = dataDirectory + File.separator + "metadata.txt";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(metadataPath))) {
            writer.write("# Column metadata");
            writer.newLine();
            writer.write("# Format: column_name,is_compressed");
            writer.newLine();
            
            for (String columnName : columnNames) {
                writer.write(columnName + "," + isCompressed.get(columnName));
                writer.newLine();
            }
        }
    }
    
    /**
     * Load metadata from file when opening an existing compressed column store
     */
    public void loadMetadata() throws IOException {
        String metadataPath = dataDirectory + File.separator + "metadata.txt";
        File metadataFile = new File(metadataPath);
        
        if (!metadataFile.exists()) {
            throw new IOException("Metadata file not found. Is this a valid compressed column store?");
        }
        
        try (BufferedReader reader = new BufferedReader(new FileReader(metadataPath))) {
            String line;
            columnNames.clear();
            isCompressed.clear();
            
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("#")) {
                    continue;  // Skip comments
                }
                
                String[] parts = line.split(",");
                if (parts.length >= 2) {
                    String columnName = parts[0];
                    boolean compressed = Boolean.parseBoolean(parts[1]);
                    
                    columnNames.add(columnName);
                    isCompressed.put(columnName, compressed);
                }
            }
        }
    }
    
    public List<String> getColumnData(String columnName) throws IOException {
        if (!columnNames.contains(columnName)) {
            throw new IllegalArgumentException("Column not found: " + columnName);
        }
        
        // Check if column is compressed
        if (isCompressed.getOrDefault(columnName, false)) {
            return getDecompressedColumnData(columnName);
        } else {
            String columnFilePath = dataDirectory + File.separator + columnName + ".col";
            return Files.readAllLines(Paths.get(columnFilePath));
        }
    }
    
    private List<String> getDecompressedColumnData(String columnName) throws IOException {
        // Load dictionary and get bits per value
        Map<Integer, String> reverseDictionary = new HashMap<>();
        int bitsPerValue = loadDictionary(columnName, reverseDictionary);
        
        // Now read and decompress the data
        return readCompressedData(columnName, reverseDictionary, bitsPerValue);
    }
    
    /**
     * Load dictionary for a compressed column
     */
    private int loadDictionary(String columnName, Map<Integer, String> reverseDictionary) throws IOException {
        String dictionaryPath = dataDirectory + File.separator + columnName + ".dict";
        int bitsPerValue = 0;
        
        try (BufferedReader reader = new BufferedReader(new FileReader(dictionaryPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("#")) {
                    // Parse bits per value from metadata comment
                    if (line.contains("Bits used per value:")) {
                        bitsPerValue = Integer.parseInt(line.split(":")[1].trim());
                    }
                    continue;
                }
                
                String[] parts = line.split(",");
                if (parts.length >= 2) {
                    // Note: reversed from original - now it's index -> value
                    reverseDictionary.put(Integer.parseInt(parts[1]), parts[0]);
                }
            }
        }
        
        if (bitsPerValue == 0) {
            throw new IOException("Invalid dictionary file: missing bits per value metadata");
        }
        
        return bitsPerValue;
    }
    
    /**
     * Read compressed data and decompress it using the dictionary
     */
    private List<String> readCompressedData(String columnName, Map<Integer, String> reverseDictionary, 
                                           int bitsPerValue) throws IOException {
        String compressedFilePath = dataDirectory + File.separator + columnName + ".cmp";
        List<String> result = new ArrayList<>();
        
        BitInputStream bis = null;
        try {
            bis = new BitInputStream(new FileInputStream(compressedFilePath));
            
            // Read metadata
            int storedBitsPerValue = bis.readInt();
            int valueCount = bis.readInt();
            
            if (storedBitsPerValue != bitsPerValue) {
                throw new IOException("Metadata mismatch between dictionary and compressed file");
            }
            
            // Read each value
            for (int i = 0; i < valueCount; i++) {
                int index = bis.readBits(bitsPerValue);
                String value = reverseDictionary.get(index);
                result.add(value);
            }
        } finally {
            if (bis != null) {
                bis.close();
            }
        }
        
        return result;
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
    
    // Helper class to write bits to a file
    private static class BitOutputStream implements Closeable {
        private OutputStream out;
        private int buffer;
        private int bitsInBuffer;
        
        public BitOutputStream(OutputStream out) {
            this.out = out;
            this.buffer = 0;
            this.bitsInBuffer = 0;
        }
        
        public void writeBits(int value, int numBits) throws IOException {
            if (numBits <= 0) return;
            
            // Make sure value fits in numBits
            int mask = (1 << numBits) - 1;
            value &= mask;
            
            while (numBits > 0) {
                // How many bits we can add to the buffer
                int bitsToAdd = Math.min(8 - bitsInBuffer, numBits);
                
                // Shift value and add to buffer
                buffer = (buffer << bitsToAdd) | (value >>> (numBits - bitsToAdd));
                bitsInBuffer += bitsToAdd;
                numBits -= bitsToAdd;
                
                // If buffer is full, write it out
                if (bitsInBuffer == 8) {
                    out.write(buffer);
                    buffer = 0;
                    bitsInBuffer = 0;
                }
                
                // Prepare for next iteration
                value &= (1 << numBits) - 1;
            }
        }
        
        public void writeInt(int value) throws IOException {
            // Write a 32-bit integer directly to the stream
            out.write((value >>> 24) & 0xFF);
            out.write((value >>> 16) & 0xFF);
            out.write((value >>> 8) & 0xFF);
            out.write(value & 0xFF);
        }
        
        @Override
        public void close() throws IOException {
            // Flush any remaining bits
            if (bitsInBuffer > 0) {
                buffer <<= (8 - bitsInBuffer);
                out.write(buffer);
            }
            out.close();
        }
    }
    
    // Helper class to read bits from a file
    private static class BitInputStream implements Closeable {
        private InputStream in;
        private int buffer;
        private int bitsInBuffer;
        
        public BitInputStream(InputStream in) {
            this.in = in;
            this.buffer = 0;
            this.bitsInBuffer = 0;
        }
        
        public int readBits(int numBits) throws IOException {
            if (numBits <= 0) return 0;
            
            int result = 0;
            int bitsStillNeeded = numBits;
            
            while (bitsStillNeeded > 0) {
                // If buffer is empty, fill it
                if (bitsInBuffer == 0) {
                    int nextByte = in.read();
                    if (nextByte == -1) {
                        throw new EOFException("Unexpected end of file");
                    }
                    buffer = nextByte;
                    bitsInBuffer = 8;
                }
                
                // How many bits we can take from buffer
                int bitsToTake = Math.min(bitsInBuffer, bitsStillNeeded);
                
                // Extract bits from buffer
                int shift = bitsInBuffer - bitsToTake;
                int mask = ((1 << bitsToTake) - 1) << shift;
                int extractedBits = (buffer & mask) >>> shift;
                
                // Add to result
                result = (result << bitsToTake) | extractedBits;
                
                // Update state
                bitsInBuffer -= bitsToTake;
                bitsStillNeeded -= bitsToTake;
            }
            
            return result;
        }
        
        public int readInt() throws IOException {
            // Read a 32-bit integer directly from the stream
            int b1 = in.read();
            int b2 = in.read();
            int b3 = in.read();
            int b4 = in.read();
            
            if ((b1 | b2 | b3 | b4) < 0) {
                throw new EOFException("Unexpected end of file");
            }
            
            return (b1 << 24) | (b2 << 16) | (b3 << 8) | b4;
        }
        
        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}