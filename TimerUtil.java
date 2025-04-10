import java.util.function.Supplier;

public class TimerUtil {

    // Functional interface that allows throwing checked exceptions
    @FunctionalInterface
    public interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    // Wrapper for result and time taken
    public static class TimedResult<T> {
        private final T result;
        private final double durationMs;

        public TimedResult(T result, double durationMs) {
            this.result = result;
            this.durationMs = durationMs;
        }

        public T getResult() {
            return result;
        }

        public double getDurationMs() {
            return durationMs;
        }

        @Override
        public String toString() {
            return "Result: " + result + ", Duration: " + durationMs + " ms";
        }
    }

    // Main timing function that works even with methods that throw
    public static <T> TimedResult<T> timeFunction(ThrowingSupplier<T> function) {
        long start = System.nanoTime();
        try {
            T result = function.get();
            long end = System.nanoTime();
            return new TimedResult<>(result, (end - start) / 1_000_000.0);
        } catch (Exception e) {
            throw new RuntimeException("Function threw an exception", e);
        }
    }
}
