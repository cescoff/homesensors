import com.google.common.collect.Lists;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CSVMerger {

    // 28/01/2020 10:34:30

    private static final Pattern DATETIME_PATTERN = Pattern.compile("([0-9]+/[0-9]+/[0-9]+\\s+[0-9+]+:[0-9]+:[0-9]+)");
    private static final Pattern POSTAL_CODE_PATTERN = Pattern.compile("(\"[0-9]+-[0-9]+\")");
    private static final DateTimeFormatter DATE_TIME_FORMAT_1 = DateTimeFormat.forPattern("dd/MM/yyyy H:m:s");
    private static final DateTimeFormatter DATE_TIME_FORMAT_2 = DateTimeFormat.forPattern("MM/dd/yyyy H:m:s");

    public static void main(String[] args) throws Exception {
        if (!DATETIME_PATTERN.matcher(",\"false\",\"\",\"\",\"false\",\"false\",\"28/01/2020 10:34:30\",\"false\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"ref:_00Db0diJz._5000X1W4ZW4:ref\",\"\",\"\"").find()) {
            System.out.println("ERR");
            return;
        }
        final List<File> files = Lists.newArrayList(
                new File("/Users/corentin/Documents/Customers/Evernex/01_case_extract.csv"),
                new File("/Users/corentin/Documents/Customers/Evernex/02_case_extract.csv"),
                new File("/Users/corentin/Documents/Customers/Evernex/03_case_extract.csv"));

        final FileOutputStream fos = new FileOutputStream(new File("/Users/corentin/Documents/Customers/Evernex/case_extract.csv"));

        boolean headerDone = false;
        for (final File file : files) {
            final FileInputStream fileInputStream = new FileInputStream(file);
            final LineIterator lineIterator = new LineIterator(new InputStreamReader(fileInputStream));

            final String header = lineIterator.nextLine();

            if (!headerDone) {
                fos.write(new StringBuilder(header).append("\n").toString().getBytes());
                headerDone = true;
            }

            while (lineIterator.hasNext()) {
                String line = lineIterator.nextLine();

                Matcher dateMatcher = DATETIME_PATTERN.matcher(line);

                while (dateMatcher.find()) {
                    final String dateTime = dateMatcher.group(1);
                    try {
                        line = StringUtils.replace(line, dateTime, DATE_TIME_FORMAT_1.parseLocalDateTime(dateTime).toString());
                    } catch (Throwable t1) {
                        try {
                            line = StringUtils.replace(line, dateTime, DATE_TIME_FORMAT_2.parseLocalDateTime(dateTime).toString());
                        } catch (Throwable t2) {
                            line = StringUtils.replace(line, dateTime, "");
                            System.out.println("Wrong date '" + dateTime + "'");
                        }
                    }
                    dateMatcher = DATETIME_PATTERN.matcher(line);
                }

                Matcher postalCodeMatcher = POSTAL_CODE_PATTERN.matcher(line);
                if (postalCodeMatcher.find()) {
                    final String postalCode = postalCodeMatcher.group(1);
                    line = StringUtils.replace(line, postalCode, StringUtils.remove(postalCode, "-"));
                }

                fos.write(new StringBuilder(line).append("\n").toString().getBytes());
            }
            fileInputStream.close();
        }
        fos.close();
    }

}
