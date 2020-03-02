import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.Locale;


public class TestDateTime {

    // 550 -> 555

    public static void main(String[] args) throws ParseException {
        final DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
        final LocalDateTime start = LocalDateTime.now().minusDays(555);
        final LocalDateTime end = LocalDateTime.now().minusDays(550);

        System.out.println("Period from " + dateTimeFormatter.print(start) + " to " + dateTimeFormatter.print(end));

        final Date date = new Date();
        System.out.println(date.toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDate().getMonthValue());

        final java.text.SimpleDateFormat formatter = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        System.out.println(LocalDateTime.now().plusDays(52));

    }

}
