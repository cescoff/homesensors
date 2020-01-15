import org.joda.time.LocalDateTime;
import org.joda.time.Period;


public class TestPeriod {

    public static void main(String[] args) {
        final LocalDateTime start = LocalDateTime.now().minusDays(10).minusMinutes(20);
        final LocalDateTime end = LocalDateTime.now();
        System.out.println(start);
        System.out.println(end);
        System.out.println(new Period(start, end).getDays());
    }

}
