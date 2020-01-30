import com.desi.data.SensorRecord;
import com.desi.data.aggregation.HeatBurnAggregator;
import com.desi.data.bean.TemperatureRecord;
import com.desi.data.impl.StaticSensorNameProvider;
import com.desi.data.utils.LogUtils;
import com.desi.data.utils.TemperatureCSVParser;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.joda.time.LocalDateTime;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;

public class HeatBurnGeneratorTest {

    public static void main(String[] args) throws Exception {
        LogUtils.configure("test.log");

        final List<SensorRecord> allRecords = Lists.newArrayList();
        final List<File> files = Lists.newArrayList(new File("/Users/corentin/Downloads/external_temperatures.txt"), new File("/Users/corentin/Downloads/2020-01-26_external_temperatures.txt"), new File("/Users/corentin/Downloads/2020-01-28_external_temperatures.txt"));
        for (final File csvFile : files) {
            final FileInputStream fileInputStream = new FileInputStream(csvFile);
            Iterables.addAll(allRecords, TemperatureCSVParser.parseContent(fileInputStream));
        }
        final HeatBurnAggregator aggregator1 = new HeatBurnAggregator(new LocalDateTime("2020-01-25T10:28:00"), new LocalDateTime("2020-01-25T11:28:00"), new StaticSensorNameProvider());
        final HeatBurnAggregator aggregator2 = new HeatBurnAggregator(new LocalDateTime("2020-01-25T12:16:00"), new LocalDateTime("2020-01-25T14:45:00"), new StaticSensorNameProvider());
        final HeatBurnAggregator aggregator3 = new HeatBurnAggregator(new LocalDateTime("2020-01-27T23:32:00"), new LocalDateTime("2020-01-28T00:25:00"), new StaticSensorNameProvider());
        final HeatBurnAggregator aggregator4 = new HeatBurnAggregator(new LocalDateTime("2020-01-28T00:25:00"), new LocalDateTime("2020-01-28T02:02:00"), new StaticSensorNameProvider());
        final HeatBurnAggregator aggregator5 = new HeatBurnAggregator(new LocalDateTime("2020-01-28T02:02:00"), new LocalDateTime("2020-01-28T07:32:00"), new StaticSensorNameProvider());
        final HeatBurnAggregator aggregator6 = new HeatBurnAggregator(new LocalDateTime("2020-01-28T07:32:00"), new LocalDateTime("2020-01-28T08:23:00"), new StaticSensorNameProvider());
        for (final SensorRecord record : allRecords) {
            aggregator1.add(record);
            aggregator2.add(record);
            aggregator3.add(record);
            aggregator4.add(record);
            aggregator5.add(record);
            aggregator6.add(record);
        }

        int position = 1;
        for (final HeatBurnAggregator heatBurnAggregator : Lists.newArrayList(aggregator1, aggregator2, aggregator3, aggregator4, aggregator5, aggregator6)) {
            final Map<String, Iterable<SensorRecord>> heatBurns = heatBurnAggregator.compute();

            for (final String uuid : heatBurns.keySet()) {
                float value = 0;
                for (final SensorRecord record : heatBurns.get(uuid)) {
                    value+=record.getValue();
                    System.out.println("Aggregator" + position + " '" + uuid + "' Burn '" + record.getDateTaken() + "' value is " + record.getValue());
                }
                System.out.println("Aggregator" + position + " '" + uuid + "' SUM value is " + value);
            }
            position++;
        }


    }

}
