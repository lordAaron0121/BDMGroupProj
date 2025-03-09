import java.io.*;

public class ReadMonthColumnOnly {
    public static void main(String[] args) throws Exception {
        // Path to the JSON file
        String jsonFilePath = "output.json";

        // Step 1: Open the file for reading
        BufferedReader reader = new BufferedReader(new FileReader(jsonFilePath));
        String line;
        boolean isInsideMonthArray = false;
        StringBuilder monthData = new StringBuilder();

        // Step 2: Read line by line
        while ((line = reader.readLine()) != null) {
            // Check if we have entered the "month" column
            if (line.contains("\"month\":")) {
                isInsideMonthArray = true;  // Start reading the "month" data
            }

            // If inside the "month" array, collect the month values
            if (isInsideMonthArray) {
                // Check for the opening square bracket of the "month" array
                if (line.contains("[")) {
                    // Remove everything before the first value in the array
                    int startIdx = line.indexOf("[") + 1;
                    line = line.substring(startIdx).trim();
                }

                // If the line ends with a closing bracket, we need to finish reading
                if (line.contains("]")) {
                    int endIdx = line.indexOf("]");
                    line = line.substring(0, endIdx).trim();
                    isInsideMonthArray = false; // End of the "month" array
                }

                // Extract the values (assuming CSV-like format)
                String[] months = line.split(",");
                for (String month : months) {
                    monthData.append(month.replace("\"", "").trim()).append("\n");
                }

                // If we've finished reading the "month" data, we can stop reading further
                if (!isInsideMonthArray) {
                    break; // Exit once the month data is collected
                }
            }
        }

        reader.close();

        // Step 3: Print the "month" column data
        if (monthData.length() > 0) {
            System.out.println("Month Column Data:");
            System.out.println(monthData.toString());
        } else {
            System.out.println("No 'month' column found.");
        }
    }
}
