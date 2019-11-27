package com.desi.data.bigquery;


import org.joda.time.LocalDateTime;

public enum AggregationScope {

    MINUTE(1),
    HOUR(2),
    DAY(3);

    private final int id;

    private AggregationScope(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public LocalDateTime nextValue(final LocalDateTime previous) {
        if (id == 1) {
            return previous.plusMinutes(10);
        } else if (id == 2) {
            return previous.plusHours(1);
        } else if (id == 3) {
            return previous.plusDays(1);
        }
        throw new IllegalStateException("Unsupported scope " + id);
    }

    public LocalDateTime previousValue(final LocalDateTime current) {
        if (id == 1) {
            return current.minusMinutes(current.getMinuteOfHour() % 10);
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

}