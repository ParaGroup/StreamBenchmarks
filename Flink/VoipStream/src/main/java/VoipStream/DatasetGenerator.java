package VoipStream;

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class DatasetGenerator {
    private List<String> numberPool;
    private long calls;

    private static String randomTelephoneNumber() {
        return "" + (10_000_000_000L + ThreadLocalRandom.current().nextLong(90_000_000_000L));
    }

    public DatasetGenerator(long numbers, long calls) {
        assert numbers > 1;

        this.calls = calls;
        numberPool = new ArrayList<>();
        for (long i = 0; i < numbers; i++) {
            numberPool.add(randomTelephoneNumber());
        }
    }

    private String pickRandomNumber() {
        return pickRandomNumber(null);
    }

    private String pickRandomNumber(String not) {
        String number;
        do {
            int index = ThreadLocalRandom.current().nextInt(numberPool.size());
            number = numberPool.get(index);
        } while (number.equals(not));
        return number;
    }

    public void generate() {
        for (long i = 0; i < calls; i++) {
            String calling = pickRandomNumber();
            String called = pickRandomNumber(calling);
            DateTime answerTimestamp = DateTime.now();
            int callDuration = ThreadLocalRandom.current().nextInt(3600 * 24);
            boolean callEstablished = ThreadLocalRandom.current().nextDouble() >= 0.05;

            System.out.printf("%s,%s,%s,%d,%b\n", calling, called, answerTimestamp, callDuration, callEstablished);
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: <numbers> <calls>");
            System.exit(1);
        }

        long numbers = Long.parseUnsignedLong(args[0]);
        long calls = Long.parseUnsignedLong(args[1]);
        DatasetGenerator generator = new DatasetGenerator(numbers, calls);
        generator.generate();
    }
}
