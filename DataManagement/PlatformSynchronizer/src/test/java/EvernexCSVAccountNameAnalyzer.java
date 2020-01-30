import com.google.common.base.Function;
import com.google.common.collect.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EvernexCSVAccountNameAnalyzer {

    public static void main(String[] args) throws Exception {
        final File accountFile = new File("/Users/corentin/Documents/Customers/Evernex/account_extract.csv");
        final CSVParser parser = CSVParser.parse(accountFile, StandardCharsets.UTF_8, CSVFormat.EXCEL.withHeader().withDelimiter(','));

        final Map<String, List<CSVRecord>> nameIndex = Maps.newHashMap();
        final Map<String, String> id2BillingCountry = Maps.newHashMap();
        final Map<String, CSVRecord> id2Record = Maps.newHashMap();

        for (final CSVRecord record : parser.getRecords()) {
            final String nameCandidate = StringUtils.lowerCase(getName(record));
            if (!nameIndex.containsKey(nameCandidate)) {
                nameIndex.put(nameCandidate, Lists.newArrayList());
            }
            nameIndex.get(nameCandidate).add(record);
            id2BillingCountry.put(getId(record), getBillingCountry(record));
            id2Record.put(getId(record), record);
        }

        Set<String> validIds = Sets.newHashSet();

        final File validIdsFile = new File("/Users/corentin/Downloads/results-20200129-093046.csv");
        final FileInputStream fileInputStream = new FileInputStream(validIdsFile);
        final LineIterator lineIterator = new LineIterator(new InputStreamReader(fileInputStream));
        if (lineIterator.hasNext()) lineIterator.nextLine();

        while (lineIterator.hasNext()) {
            validIds.add(lineIterator.nextLine());
        }

        int processedCount = 0;
        int totalCount = nameIndex.keySet().size();
        Map<String, List<CSVRecord>> suspiciousRecords = Maps.newHashMap();
        for (final String name : Ordering.natural().sortedCopy(nameIndex.keySet())) {
            if (processedCount > 0 && processedCount % 1000 == 0) {
                System.out.println("Processed " + processedCount + "/" + totalCount);
            }
            processedCount++;
            for (final String otherName : nameIndex.keySet()) {
                if (StringUtils.startsWith(name, otherName)) {
                    final Set<String> records1IDs = Sets.newHashSet(Iterables.transform(nameIndex.get(name), TO_ID));
                    final Set<String> records2IDs = Sets.newHashSet(Iterables.transform(nameIndex.get(otherName), TO_ID));

                    final Set<String> exclusion = Sets.newHashSet(exclusion(records1IDs, records2IDs));

                    if (!Iterables.isEmpty(exclusion)) {
                        if (!suspiciousRecords.containsKey(otherName)) {
                            suspiciousRecords.put(otherName, Lists.newArrayList());
                        }
                        final List<CSVRecord> ids2Add = Lists.newArrayList();
                        final Set<String> alreadyAddedIds = Sets.newHashSet(Iterables.transform(suspiciousRecords.get(otherName), TO_ID));
                        for (final String id : exclusion) {
                            if (!alreadyAddedIds.contains(id)) {
                                ids2Add.add(id2Record.get(id));
                            }
                        }
                        suspiciousRecords.get(otherName).addAll(ids2Add);
                    }

                }
            }
        }

        int duplicateCountWithValidOrganisation = 0;
        Set<String> between2And50UniqueIds = Sets.newHashSet();
        Set<String> moreThan50UniqueIds = Sets.newHashSet();
        final FileOutputStream csvOut = new FileOutputStream(new File("/Users/corentin/Downloads/largeDuplicateCounts.csv"));
        csvOut.write(new String("\"Name\",\"Count\"\n").getBytes());
        for (final String name : Ordering.natural().sortedCopy(suspiciousRecords.keySet())) {
            if (suspiciousRecords.get(name).size() > 50) {
                int invalidCount = 0;
                for (CSVRecord csvRecord : suspiciousRecords.get(name)) {
                    if (!validIds.contains(getId(csvRecord))) {
                        invalidCount++;
                        moreThan50UniqueIds.add(getId(csvRecord));
                    }
                }
                csvOut.write(new String("\"" + name + "\",\"" + suspiciousRecords.get(name).size() + "\"\n").getBytes());
            } else if (suspiciousRecords.get(name).size() > 2 && suspiciousRecords.get(name).size() <= 50) {
                int invalidCount = 0;
                for (CSVRecord csvRecord : suspiciousRecords.get(name)) {
                    if (!validIds.contains(getId(csvRecord))) {
                        invalidCount++;
                        between2And50UniqueIds.add(getId(csvRecord));
                    }
                }
                if (invalidCount == 1) {
                    duplicateCountWithValidOrganisation++;
                }
            }
        }
        csvOut.close();
        System.out.println("More than 50 : " + moreThan50UniqueIds.size());
        System.out.println("Between 2 and 50 " + between2And50UniqueIds.size());
        System.out.println("Duplicate count " + duplicateCountWithValidOrganisation);
    }

    private static <T> Iterable<T> intersection(final Iterable<T> i, final Iterable<T> j) {
        final List<T> result = Lists.newArrayList();
        for (final T ii : i) {
            for (final T jj : j) {
                if (ii.equals(jj)) {
                    result.add(ii);
                }
            }
        }
        return result;
    }

    private static <T> Iterable<T> exclusion(final Iterable<T> i, final Iterable<T> j) {
        final Set<T> intersection = Sets.newHashSet(intersection(i, j));
        final List<T> result = Lists.newArrayList();
        for (final T ii : i) if (!intersection.contains(ii)) result.add(ii);
        for (final T jj : j) if (!intersection.contains(jj)) result.add(jj);
        return result;
    }

    private static final Function<CSVRecord, String> TO_ID = new Function<CSVRecord, String>() {
        @Nullable
        @Override
        public String apply(@Nullable CSVRecord strings) {
            return getId(strings);
        }
    };

    private static String getId(final CSVRecord record) {
        return record.get("ID");
    }

    private static String getBillingCountry(final CSVRecord record) {
        return record.get("BILLINGCOUNTRY");
    }

    private static String getName(final CSVRecord record) {
        return record.get("NAME");
    }

}
