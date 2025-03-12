import java.util.*;

public class ZoneMapStore {
    private final List<Record> records;
    private final List<Zone> zones;
    private final int zoneSize;
    private int totalZones = 0;
    private int prunedZones = 0;

    public ZoneMapStore(int zoneSize) {
        this.zoneSize = zoneSize;
        this.records = new ArrayList<>();
        this.zones = new ArrayList<>();
    }

    private static class Zone {
        // For area
        double minArea = Double.MAX_VALUE;
        double maxArea = Double.MIN_VALUE;
        // For months (store as strings for direct comparison)
        String minMonth = "9999-99";
        String maxMonth = "0000-00";
        // For town (we'll use a bitmap for faster lookups)
        Set<String> towns = new HashSet<>();
        int startIndex;
        int endIndex;

        void addRecord(Record record) {
            minArea = Math.min(minArea, record.area);
            maxArea = Math.max(maxArea, record.area);
            minMonth = record.month.compareTo(minMonth) < 0 ? record.month : minMonth;
            maxMonth = record.month.compareTo(maxMonth) > 0 ? record.month : maxMonth;
            towns.add(record.town);
        }

        boolean canPrune(String targetMonth1, String targetMonth2, String targetTown, double minArea) {
            // Prune if:
            // 1. Zone's max area is less than required min area
            // 2. Zone's month range doesn't overlap with target months
            // 3. Zone doesn't contain the target town
            if (maxArea < minArea) return true;
            
            if (maxMonth.compareTo(targetMonth1) < 0 && maxMonth.compareTo(targetMonth2) < 0) return true;
            if (minMonth.compareTo(targetMonth1) > 0 && minMonth.compareTo(targetMonth2) > 0) return true;
            
            return !towns.contains(targetTown);
        }
    }

    public static class Record {
        final String month;
        final String town;
        final double area;
        final double price;

        public Record(String month, String town, double area, double price) {
            this.month = month;
            this.town = town;
            this.area = area;
            this.price = price;
        }
    }

    public void loadFromColumnStore(ColumnStore store) {
        List<Object> months = store.getColumn("month");
        List<Object> towns = store.getColumn("town");
        List<Object> areas = store.getColumn("floor_area_sqm");
        List<Object> prices = store.getColumn("resale_price");

        // First, load all records
        for (int i = 0; i < months.size(); i++) {
            Record record = new Record(
                (String) months.get(i),
                (String) towns.get(i),
                Double.parseDouble((String) areas.get(i)),
                Double.parseDouble((String) prices.get(i))
            );
            records.add(record);
        }

        // Create zones
        for (int i = 0; i < records.size(); i += zoneSize) {
            Zone zone = new Zone();
            zone.startIndex = i;
            zone.endIndex = Math.min(i + zoneSize, records.size());
            
            // Add records to zone
            for (int j = zone.startIndex; j < zone.endIndex; j++) {
                zone.addRecord(records.get(j));
            }
            
            zones.add(zone);
        }
    }

    public List<Record> filter(String targetMonth1, String targetMonth2, String targetTown, double minArea) {
        List<Record> results = new ArrayList<>();
        totalZones = zones.size();
        prunedZones = 0;
        
        long pruningTime = 0;
        long scanningTime = 0;
        long startTime = System.nanoTime();

        // Check each zone
        for (Zone zone : zones) {
            // Skip zone if it can be pruned
            long beforePrune = System.nanoTime();
            boolean shouldPrune = zone.canPrune(targetMonth1, targetMonth2, targetTown, minArea);
            pruningTime += System.nanoTime() - beforePrune;
            
            if (shouldPrune) {
                prunedZones++;
                continue;
            }

            // Check records in non-pruned zones
            long beforeScan = System.nanoTime();
            for (int i = zone.startIndex; i < zone.endIndex; i++) {
                Record record = records.get(i);
                if ((record.month.equals(targetMonth1) || record.month.equals(targetMonth2)) &&
                    record.town.equals(targetTown) &&
                    record.area >= minArea) {
                    results.add(record);
                }
            }
            scanningTime += System.nanoTime() - beforeScan;
        }

        long totalTime = System.nanoTime() - startTime;
        System.out.printf("Zone pruning stats: pruned %d out of %d zones (%.2f%%)\n", 
                         prunedZones, totalZones, (prunedZones * 100.0 / totalZones));
        System.out.printf("Time breakdown: pruning=%.2fms, scanning=%.2fms, total=%.2fms\n",
                         pruningTime / 1_000_000.0, scanningTime / 1_000_000.0, totalTime / 1_000_000.0);
        return results;
    }
} 