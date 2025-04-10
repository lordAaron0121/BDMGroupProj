import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Collections;

import javax.sql.rowset.FilteredRowSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public class ZMQueryProcessor {

    public static List<Integer> queryColumn(String columnName, List<Object> queryValues, String saveDirectory) throws IOException, ClassNotFoundException {
        ZMColumnStore store = new ZMColumnStore();
        store.zoneMap = store.loadZoneMapFromFile(saveDirectory);
    
        // Get the zones for the column
        List<Zone> zones = store.getZones(columnName);
        
        // List to store numbers of relevant zones
        List<Integer> relevantZoneNumbers = new ArrayList<>();

        // Iterate through each zone
        for (Zone zone : zones) {
            // Handle numeric comparison for zones with numeric data (Double)
            if (zone.minValue instanceof Double && zone.maxValue instanceof Double) {
                double zoneMinValue = (Double) zone.minValue;
                double zoneMaxValue = (Double) zone.maxValue;

                // Iterate over the query values to check if any match
                for (Object queryValue : queryValues) {
                    if (queryValue instanceof Double) {
                        double queryDouble = (Double) queryValue;
                        if (zoneMinValue == queryDouble || zoneMaxValue == queryDouble || (zoneMinValue <= queryDouble && zoneMaxValue >= queryDouble)) {
                            relevantZoneNumbers.add((int) Math.floor(zone.startIndex/1000));
                            break;  // No need to check further if this zone is already relevant
                        }
                    }
                }
            }
            // Handle string-based zones
            else if (zone.minValue instanceof String && zone.maxValue instanceof String) {
                String zoneMinValue = (String) zone.minValue;
                String zoneMaxValue = (String) zone.maxValue;

                // Iterate over the query values to check if any match
                for (Object queryValue : queryValues) {
                    if (queryValue instanceof String) {
                        String queryString = (String) queryValue;
                        if (zoneMinValue.equals(queryString) || zoneMaxValue.equals(queryString) || 
                            (zoneMinValue.compareTo(queryString) <= 0 && zoneMaxValue.compareTo(queryString) >= 0)) {
                                relevantZoneNumbers.add((int) Math.floor(zone.startIndex/1000));
                                break;  // No need to check further if this zone is already relevant
                        }
                    }
                }
            }
        }

        return relevantZoneNumbers;
    }

    // Helper method to read zone data from the corresponding zone file on disk
    private static List<Object> readZoneDataFromFile(File zoneFile) throws IOException {
        List<Object> zoneData = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(zoneFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                zoneData.add(line);  // Assuming the data is stored as a string, you can parse as necessary
            }
        }
        return zoneData;
    }



    public static void runQuery() {}

    public static void main(String[] args) {
        String filePath = "data/ResalePricesSingapore.csv";
        String saveDirectory = "ZoneMap";  // Specify the output directory where zones and zone map will be stored

        try {
            // // Step 1: Load CSV data and partition into zones
            // ZMColumnStore store = ZMCSVLoader.loadCSV(filePath, saveDirectory);

            // System.out.println("Data processing complete. Zones saved to disk.");
            

            // TO TIME THIS PART
            // Reading data from disk
            long startTime = System.nanoTime();

            // Step 2: Get relevant zones based on the 'month' column (checking if the month is '2016-04' or '2016-05')
            List<Object> queryValues_month = Arrays.asList("2016-04", "2016-05");
            List<Integer> relevantZoneNumbers_month = queryColumn("month", queryValues_month, saveDirectory);

            List<Object> queryValues_town = Arrays.asList("CHOA CHU KANG");
            List<Integer> relevantZoneNumbers_town = queryColumn("town", queryValues_town, saveDirectory);

            relevantZoneNumbers_month.retainAll((relevantZoneNumbers_town));

            List<Double> filteredPrices = new ArrayList<>();

            // Step 3: Process the relevant zones and read the corresponding zone data from disk
            for (Integer zoneNumber : relevantZoneNumbers_month) {
                // Step 4: Read zone data from the zone file and process it
                // Construct the file path for the zone text file
                File zoneFileMonth = new File(saveDirectory, "month_zone_" + zoneNumber + ".txt");
                List<Object> zoneDataMonth = readZoneDataFromFile(zoneFileMonth);

                File zoneFileTown = new File(saveDirectory, "town_zone_" + zoneNumber + ".txt");
                List<Object> zoneDataTown = readZoneDataFromFile(zoneFileTown);

                File zoneFileFloor = new File(saveDirectory, "floor_area_sqm_zone_" + zoneNumber + ".txt");
                List<Object> zoneDataFloor = readZoneDataFromFile(zoneFileFloor);

                File zoneFilePrice = new File(saveDirectory, "resale_price_zone_" + zoneNumber + ".txt");
                List<Object> zoneDataPrice = readZoneDataFromFile(zoneFilePrice);

                System.out.println("Processing Zone : " + zoneNumber);
                
                // Process the zone data here
                // For example, you could print the zone data or perform further computations.
                for (int i=0; i<1000; i++) {
                    Object currentMonth = zoneDataMonth.get(i);
                    if ("2016-04".equals(currentMonth) || "2016-05".equals(currentMonth)) {
                        if ("CHOA CHU KANG".equals(zoneDataTown.get(i))) {
                            if (Double.parseDouble(zoneDataFloor.get(i).toString()) >= 80) {
                                filteredPrices.add(Double.parseDouble(zoneDataPrice.get(i).toString()));
                                System.out.println(zoneNumber*1000+i);
                            }
                        }
                    }
                }
            }
            System.out.println(filteredPrices);

            System.out.println(Collections.min(filteredPrices));

            long totalTime = (System.nanoTime() - startTime) / 1_000_000;

            System.out.println(totalTime);

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("An error occurred during processing: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
