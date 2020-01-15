package com.desi.data.config;

import com.desi.data.CarSensorRecord;
import com.desi.data.bean.AnnotatedImage;
import com.desi.data.bean.VehicleImageData;
import com.google.common.base.Optional;

public interface CarConfiguration {

    public String getName();

    public String getUUID();

    public String getDistanceUUID(final CarSensorRecord startPosition, final CarSensorRecord endPosition);

    public String getOdometerUUID();

    public String getGasolineMeterUUID();

    public String getGPSUUID();

    public String getGasolineUUID(final CarSensorRecord startPosition, final CarSensorRecord endPosition);

    public String getGasolineAvgConsumptionUUID(final CarSensorRecord startPosition, final CarSensorRecord endPosition);

    public boolean isValidGasolineVolume(final float value);

    public boolean isValidGasolineConsumption(final float value);

    public boolean isValidDistanceBetween2ReFuel(final float value);

    public boolean isValidGasolinePricePerLitre(final float value);

    public boolean isValidReFuelFullPrice(final float value);

    public Optional<Float> getOdometerValue(final AnnotatedImage image);

    public boolean isVehicleInImage(final AnnotatedImage image);

    public boolean isVehicleInATrip(final CarSensorRecord carSensorRecord);

}
