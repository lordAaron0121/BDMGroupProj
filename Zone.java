import java.io.Serializable;

public class Zone implements Serializable {
    private static final long serialVersionUID = 1L; // Optional: for version control

    Object minValue;
    Object maxValue;
    int startIndex;
    int endIndex;

    public Zone(Object minValue, Object maxValue, int startIndex, int endIndex) {
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    // Optional: getters and setters
    public Object getMinValue() {
        return minValue;
    }

    public void setMinValue(double minValue) {
        this.minValue = minValue;
    }

    public Object getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(double maxValue) {
        this.maxValue = maxValue;
    }

    public int getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(int startIndex) {
        this.startIndex = startIndex;
    }

    public int getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(int endIndex) {
        this.endIndex = endIndex;
    }
}
