package com.desi.data.config;

public interface CarConfiguration {

    public String getOdometerUUID();

    public String getGasolineMeterUUID();

    public String getGPSUUID();

    public String getGasolineAvgConsumptionUUID();

    public boolean isValidGasolineVolume(final float value);

    public boolean isValidGasolineConsumption(final float value);

    public boolean isValidDistanceBetween2ReFuel(final float value);

}
