package com.desi.data.config;

import com.desi.data.bean.AnnotatedImage;
import com.google.common.base.Optional;

public interface CarConfiguration {

    public String getUUID();

    public String getDistanceUUID();

    public String getOdometerUUID();

    public String getGasolineMeterUUID();

    public String getGPSUUID();

    public String getGasolineAvgConsumptionUUID();

    public boolean isValidGasolineVolume(final float value);

    public boolean isValidGasolineConsumption(final float value);

    public boolean isValidDistanceBetween2ReFuel(final float value);

    public boolean isValidGasolinePricePerLitre(final float value);

    public boolean isValidReFuelFullPrice(final float value);

    public Optional<Float> getOdometerValue(final AnnotatedImage image);

}
