package com.desi.data.bigquery;


import org.javatuples.Pair;
import org.joda.time.LocalDateTime;

public enum AggregationScope {

    MINUTE(1),
    HOUR(2),
    DAY(3),
    SLIDING(4);

    private final int id;

    private AggregationScope(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public LocalDateTime getStartDateTime(final LocalDateTime checkpointValue) {
        if (id != 4) {
            return checkpointValue;
        }
        return checkpointValue.minusMinutes(50);
    }

    public LocalDateTime nextValue(final LocalDateTime previous) {
        if (id == 1 || id == 4) {
            return previous.plusMinutes(10);
        } else if (id == 2) {
            return previous.plusHours(1);
        } else if (id == 3) {
            return previous.plusDays(1);
        }
        throw new IllegalStateException("Unsupported scope " + id);
    }

    public LocalDateTime previousValue(final LocalDateTime current) {
        if (id == 1 || id == 4) {
            return current.minusMinutes(10 + current.getMinuteOfHour() % 10).withSecondOfMinute(0).withMillisOfSecond(0);
        } else if (id == 2) {
            return current.minusHours(1).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
        } else if (id == 3) {
            return current.minusDays(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
        }
        throw new IllegalStateException("Unsupported scope " + id);
    }

    public boolean isEndOfWindow(final LocalDateTime localDateTime) {
        final LocalDateTime previousValue = previousValue(LocalDateTime.now());
        return localDateTime.isAfter(previousValue);
    }

    public Pair<LocalDateTime, LocalDateTime> getNextPeriod(final LocalDateTime previousEnd) {
        if (id != 4) {
            return Pair.with(previousEnd, nextValue(previousEnd));
        }
        final LocalDateTime next = nextValue(previousEnd);
        return Pair.with(next.minusMinutes(30), next);
    }

}
