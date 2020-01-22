package com.desi.data.bean;

import com.desi.data.binding.FuelType;
import com.google.common.base.Optional;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;

public interface IGasStation {

    public float getLatitude();

    public float getLongitude();

    public Optional<Float> getFuelPrice(final FuelType fuelType, final LocalDateTime dateTaken);

    public String getAddress();

    public int getYearPrice();

}
