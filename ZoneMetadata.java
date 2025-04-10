import java.io.BufferedReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class ZoneMetadata implements Serializable {
    private String minValue;
    private String maxValue;
    private long startByte;
    private long endByte;

    public ZoneMetadata(String minValue, String maxValue, long startByte, long endByte) {
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.startByte = startByte;
        this.endByte = endByte;
    }

    public String getMinValue() {
        return minValue;
    }

    public void setMinValue(String minValue) {
        this.minValue = minValue;
    }

    public String getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(String maxValue) {
        this.maxValue = maxValue;
    }

    public long getStartByte() {
        return startByte;
    }

    public void setStartByte(long startByte) {
        this.startByte = startByte;
    }

    public long getEndByte() {
        return endByte;
    }

    public void setEndByte(long endByte) {
        this.endByte = endByte;
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%d,%d", minValue, maxValue, startByte, endByte);
    }

    public static List<ZoneMetadata> readZoneMetadata(String columnName, String dataDirectory) throws IOException {
        List<ZoneMetadata> zoneMetadataList = new ArrayList<>();
        Path zoneMetadataFilePath = Paths.get(dataDirectory, columnName + "_zone_map.txt");

        try (BufferedReader reader = Files.newBufferedReader(zoneMetadataFilePath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                String max = parts[0];
                String min = parts[1];
                long startBytePosition = Long.parseLong(parts[2]);
                long endBytePosition = Long.parseLong(parts[3]);

                // Create a new ZoneMetadata object and add to the list
                zoneMetadataList.add(new ZoneMetadata(min, max, startBytePosition, endBytePosition));
            }
        }

        return zoneMetadataList;
    }

    public static List<Integer> getRelevantZoneIndexes(List<ZoneMetadata> zones, String value) {
        List<Integer> relevantIndexes = new ArrayList<>();

        // Iterate through each ZoneMetadata object
        for (int i = 0; i < zones.size(); i++) {
            ZoneMetadata zone = zones.get(i);

            // Compare the value lexicographically with the zone's min and max values
            if (value.compareTo(zone.getMinValue()) >= 0 && value.compareTo(zone.getMaxValue()) <= 0) {
                relevantIndexes.add(i); // Add index of relevant zone
            }
        }

        return relevantIndexes;
    }

    public static List<Integer> getIntersection(List<List<Integer>> lists) {
        if (lists == null || lists.size() == 0) {
            return new ArrayList<>();
        }

        // Start with the first list as the initial intersection
        Set<Integer> intersection = new HashSet<>(lists.get(0));

        // Iterate through the remaining lists and update the intersection
        for (int i = 1; i < lists.size(); i++) {
            // Create a set for the current list
            Set<Integer> currentSet = new HashSet<>(lists.get(i));
            intersection.retainAll(currentSet); // Retain only common elements
        }

        // Convert the result set back to a List and return
        return new ArrayList<>(intersection);
    }

    public static Map<String, List<Integer>> getDataFromRelevantZones(String yearMonth, String town, String dataDirectory) throws IOException {        
        // Calculate the next month for the range (manually, without using YearMonth)
        String[] parts = yearMonth.split("-");
        String year = parts[0];
        String month = parts[1];
        
        // Simple calculation for next month
        String nextMonthStr;
        if (month.equals("12")) {
            // If December, next month is January of next year
            int nextYear = Integer.parseInt(year) + 1;
            nextMonthStr = nextYear + "-01";
        } else {
            // Otherwise, just increment the month
            int nextMonth = Integer.parseInt(month) + 1;
            // Ensure two digits for month
            nextMonthStr = year + "-" + (nextMonth < 10 ? "0" + nextMonth : String.valueOf(nextMonth));
        }

        List<ZoneMetadata> yearMonthZones = readZoneMetadata("month", dataDirectory);
        Set<Integer> unionSet = new HashSet<>();
        unionSet.addAll(getRelevantZoneIndexes(yearMonthZones, yearMonth));
        unionSet.addAll(getRelevantZoneIndexes(yearMonthZones, nextMonthStr));
        List<Integer> yearMonthRelevantZones = new ArrayList<>(unionSet);

        List<ZoneMetadata> townZones = readZoneMetadata("town", dataDirectory);
        List<Integer> townRelevantZones = getRelevantZoneIndexes(townZones, town);

        List<List<Integer>> allRelevantZones = new ArrayList<>();
        allRelevantZones.add(yearMonthRelevantZones);
        allRelevantZones.add(townRelevantZones);

        List<Integer> filteredZones = getIntersection(allRelevantZones);
        Collections.sort(filteredZones);

        List<String> columns = Arrays.asList("month", "town", "floor_area_sqm");
        Map<String, List<String>> relevantData = new HashMap<>();

        for (String columnName : columns) {
            // Iterate through the zones and read the relevant data from the file
            List<ZoneMetadata> columnZones = readZoneMetadata(columnName, dataDirectory);
            Path columnFilePath = Paths.get(dataDirectory, columnName + ".col");
            List<String> relevantColumnData = new ArrayList<>();
            relevantData.put(columnName, relevantColumnData);

            try (RandomAccessFile file = new RandomAccessFile(columnFilePath.toFile(), "r")) {
                for (Integer zoneIndex : filteredZones) {
                    ZoneMetadata zoneMetadata = columnZones.get(zoneIndex);

                    // We know the byte positions of the zone, so let's seek to the start byte position
                    file.seek(zoneMetadata.getStartByte());

                    // Read data from the zone (between start and end byte positions)
                    long bytesToRead = zoneMetadata.getEndByte() - zoneMetadata.getStartByte();
                    byte[] dataBuffer = new byte[(int) bytesToRead];
                    file.readFully(dataBuffer);

                    // Convert the byte data to a string
                    String columnData = new String(dataBuffer);

                    // Split the data by line and filter by condition
                    String[] rows = columnData.split("\n");
                    for (String row : rows) {
                        relevantColumnData.add(row.trim());
                    }
                }
            }
        }

        List<String> monthRelevantData = relevantData.get("month");
        List<String> townRelevantData = relevantData.get("town");
        List<String> floor_area_sqmRelevantData = relevantData.get("floor_area_sqm");
        List<Integer> filteredIndices = new ArrayList<>();
        for (int i=0; i<monthRelevantData.size(); i++) {
            if (monthRelevantData.get(i).equals(yearMonth) || monthRelevantData.get(i).equals(nextMonthStr)) {
                if (townRelevantData.get(i).equals(town)) {
                    if (Double.parseDouble(floor_area_sqmRelevantData.get(i)) >= 80) {
                        filteredIndices.add(i);
                    }
                }
            }
            // break;
        }

        System.out.println("Found " + filteredIndices.size() + " matching transactions");

        Map<String, List<Integer>> result = new HashMap<>();
        result.put("zones", filteredZones);
        result.put("indices", filteredIndices);
        return result;
    }

    public static List<Double> readDoubleColumnDataInZones(String columnName, List<Integer> filteredZones, List<Integer> indices, String dataDirectory) throws IOException {
        List<String> tempValues = new ArrayList<>();
        List<Double> finalValues = new ArrayList<>();

        // Iterate through the zones and read the relevant data from the file
        List<ZoneMetadata> columnZones = ZoneMetadata.readZoneMetadata(columnName, dataDirectory);
        Path columnFilePath = Paths.get(dataDirectory, columnName + ".col");

        try (RandomAccessFile file = new RandomAccessFile(columnFilePath.toFile(), "r")) {
            for (Integer zoneIndex : filteredZones) {
                ZoneMetadata zoneMetadata = columnZones.get(zoneIndex);

                // We know the byte positions of the zone, so let's seek to the start byte position
                file.seek(zoneMetadata.getStartByte());

                // Read data from the zone (between start and end byte positions)
                long bytesToRead = zoneMetadata.getEndByte() - zoneMetadata.getStartByte();
                byte[] dataBuffer = new byte[(int) bytesToRead];
                file.readFully(dataBuffer);

                // Convert the byte data to a string
                String columnData = new String(dataBuffer);

                // Split the data by line and filter by condition
                String[] rows = columnData.split("\n");
                for (String row : rows) {
                    tempValues.add(row.trim());
                }
            }
        }

        for (Integer index: indices) {
            finalValues.add(Double.parseDouble(tempValues.get(index)));
        }

        return finalValues;
    }

}
