import java.io.*;
import java.util.*;

public class ZMColumnStore {
    public Map<String, List<Zone>> zoneMap = new HashMap<>();

    // Save the zone data to disk
    public void saveZoneToFile(String columnName, List<Object> zoneData, int zoneIndex, String directory) throws IOException {
        // Construct the file name using columnName and zoneIndex
        File zoneFile = new File(directory, columnName + "_zone_" + zoneIndex + ".txt");

        // Ensure the directory exists before writing the file
        File parentDir = zoneFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            boolean dirsCreated = parentDir.mkdirs();
            if (dirsCreated) {
                System.out.println("Created directories: " + parentDir.getAbsolutePath());
            }
        }

        // Write the zone data to the file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(zoneFile))) {
            // Iterate over each value in the zoneData list and write it to the file
            for (Object value : zoneData) {
                writer.write(value.toString());  // Convert the value to string and write
                writer.newLine();  // Write a new line for each value
            }
        }
    }

    // Save the zone map to disk
    public void saveZoneMapToFile(Map<String, List<Zone>> zoneMap, String directory) throws IOException {
        FileOutputStream fileOut = new FileOutputStream(directory + "/zoneMap.ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(zoneMap);  // Serialize the entire zone map
        out.close();
        fileOut.close();
        System.out.println("Zone map saved to: " + directory + "/zoneMap.ser");
    }

    // Load the zone map from disk
    public Map<String, List<Zone>> loadZoneMapFromFile(String directory) throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream(directory + "/zoneMap.ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Map<String, List<Zone>> zoneMap = (Map<String, List<Zone>>) in.readObject();
        in.close();
        fileIn.close();
        System.out.println("Zone map loaded from: " + directory + "/zoneMap.ser");
        return zoneMap;
    }

    // Load a specific zone from a file
    public List<Object> loadZoneFromFile(String columnName, int zoneIndex, String directory) throws IOException {
        List<Object> zoneData = new ArrayList<>();
        File zoneFile = new File(directory, columnName + "_zone_" + zoneIndex + ".txt");
        try (BufferedReader reader = new BufferedReader(new FileReader(zoneFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                zoneData.add(line);  // We assume the column data is saved as strings
            }
        }
        return zoneData;
    }

    // Add zone metadata to the zone map
    public void addZoneMap(String columnName, List<Zone> zones) {
        zoneMap.put(columnName, zones);
    }

    // Get the zones for a specific column
    public List<Zone> getZones(String columnName) {
        return zoneMap.get(columnName);
    }

    public Map<String, List<Zone>> getZoneMap() {
        return zoneMap;
    }
}
