package com.desi.data.bigquery;


import org.joda.time.LocalDateTime;

public class HeatingLevelHelper {


    private static final int LEVEL_1_MAX = 16;
    private static final int LEVEL_1_MIN = 14;

    private static final int LEVEL_2_MAX = 24;
    private static final int LEVEL_2_MIN = 22;

    private static final int LEVEL_3_MAX = 32;
    private static final int LEVEL_3_MIN = 30;

    private static final int LEVEL_4_MAX = 40;
    private static final int LEVEL_4_MIN = 38;

    private static final int LEVEL_5_MIN = 46;

    public static float getLevel(final LocalDateTime dateTaken, final float heatingValue, final float indoorValue, final float outDoorValue) {
        if (heatingValue <= 12) {
            return 0;
        }
        if (outDoorValue >= 18) {
            return 0;
        }
        if (dateTaken.getMonthOfYear() >= 6 && dateTaken.getMonthOfYear() < 9) {
            return 0;
        }

        // LEVEL 1
        if (heatingValue >= LEVEL_1_MIN && heatingValue <= LEVEL_1_MAX) {
            return 1;
        }
        if (heatingValue > LEVEL_1_MAX && heatingValue < LEVEL_2_MIN) {
            return (1 + (heatingValue - LEVEL_1_MAX) / (LEVEL_2_MIN - LEVEL_1_MAX));
        }

        // LEVEL 2
        if (heatingValue >= LEVEL_2_MIN && heatingValue <= LEVEL_2_MAX) {
            return 2;
        }
        if (heatingValue > LEVEL_2_MAX && heatingValue < LEVEL_3_MIN) {
            return (2 + (heatingValue - LEVEL_2_MAX) / (LEVEL_3_MIN - LEVEL_2_MAX));
        }

        // LEVEL 3
        if (heatingValue >= LEVEL_3_MIN && heatingValue <= LEVEL_3_MAX) {
            return 3;
        }
        if (heatingValue > LEVEL_3_MAX && heatingValue < LEVEL_4_MIN) {
            return (3 + (heatingValue - LEVEL_3_MAX) / (LEVEL_4_MIN - LEVEL_3_MAX));
        }

        // LEVEL 4
        if (heatingValue >= LEVEL_4_MIN && heatingValue <= LEVEL_4_MAX) {
            return 4;
        }
        if (heatingValue > LEVEL_4_MAX && heatingValue < LEVEL_5_MIN) {
            return (4 + (heatingValue - LEVEL_4_MAX) / (LEVEL_5_MIN - LEVEL_4_MAX));
        }

        // LEVEL 5
        if (heatingValue >= LEVEL_5_MIN) {
            return 5;
        }
        return -1;
    }

    public static void main(String[] args) {
        System.out.println(getLevel(LocalDateTime.parse("2019-7-14T12:00:00"), 31, 34, 36));
        System.out.println(getLevel(LocalDateTime.parse("2019-12-14T12:00:00"), 31, 34, 36));
        System.out.println(getLevel(LocalDateTime.parse("2019-12-14T12:00:00"), 31, 19, 2));
        System.out.println(getLevel(LocalDateTime.parse("2019-12-14T12:00:00"), 39, 19, -5));
        System.out.println(getLevel(LocalDateTime.parse("2019-12-14T12:00:00"), 36, 19, -5));
        System.out.println(getLevel(LocalDateTime.parse("2019-12-14T12:00:00"), 35, 19, -5));
        System.out.println(getLevel(LocalDateTime.parse("2019-12-14T12:00:00"), 34, 19, -5));

        for (int index = 15; index < 70; index++) {
            System.out.println(index + "°C : LEVEL=" + getLevel(LocalDateTime.parse("2019-12-14T12:00:00"), index, 19, -5));
        }

    }

}
