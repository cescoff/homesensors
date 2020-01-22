package com.desi.data.utils;

import com.desi.data.bean.IGasStation;
import com.desi.data.binding.FuelType;
import com.desi.data.binding.GasStation;
import com.desi.data.config.ConfigurationUtils;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import org.apache.commons.lang.SystemUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.LocalDateTime;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

public class GasStationCache implements Function<IGasStation, IGasStation> {

    private static final Map<String, IGasStation> CACHE = Maps.newHashMap();

    private static LocalDateTime LAST_REFRESHED = null;

    @Nullable
    @Override
    public IGasStation apply(@Nullable IGasStation iGasStation) {
        return new CachedGasStation(iGasStation);
    }

    private static class CachedGasStation implements IGasStation {

        private final float latitude;

        private final float longitude;

        private final String address;

        private final int year;

        private final File cacheFile;

        private CachedGasStation(IGasStation source) {
            this.latitude = source.getLatitude();
            this.longitude = source.getLongitude();
            this.address = source.getAddress();
            this.year = source.getYearPrice();

            final File cacheDir = new File(ConfigurationUtils.getStorageDir(), "work" + SystemUtils.FILE_SEPARATOR + "gas-station-cache" + SystemUtils.FILE_SEPARATOR + source.getYearPrice());

            if (!cacheDir.exists()) {
                LoggerFactory.getLogger(getClass()).info("Creating cache dir '" + cacheDir.getAbsolutePath() + "'");
                cacheDir.mkdirs();
            }


            this.cacheFile = new File(cacheDir, "lat_" + latitude + "-lon_" + longitude + "_" + source.getYearPrice() + ".xml");
            if (!cacheFile.exists() || (cacheFile.lastModified() < (new Date().getTime() - (24 * 3600000)))) {
                try {
                    JAXBUtils.marshal(source, cacheFile, true);
                } catch (JAXBException e) {
                    throw new IllegalStateException(e);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        }

        private IGasStation manageCache() {
            if (LAST_REFRESHED == null || LocalDateTime.now().minusMinutes(15).isAfter(LAST_REFRESHED)) {
                CACHE.clear();
                LAST_REFRESHED = LocalDateTime.now();
            }
            final String cacheKey = createCacheKey();
            if (CACHE.containsKey(cacheKey)) {
                return CACHE.get(cacheKey);
            }

            GasStation cached = null;
            try {
                cached = JAXBUtils.unmarshal(GasStation.class, this.cacheFile);
            } catch (JAXBException e) {
                throw new IllegalStateException(e);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return cached;
        }

        private String createCacheKey() {
            return new StringBuilder("latitude=").append(latitude).append("/longitude=").append(longitude).append("/year=").append(year).toString();
        }

        @Override
        public float getLatitude() {
            return latitude;
        }

        @Override
        public float getLongitude() {
            return longitude;
        }

        @Override
        public Optional<Float> getFuelPrice(FuelType fuelType, LocalDateTime dateTaken) {
            final IGasStation cached = manageCache();
            if (cached == null) {
                return Optional.absent();
            }
            return cached.getFuelPrice(fuelType, dateTaken);
        }

        @Override
        public String getAddress() {
            return address;
        }

        @Override
        public int getYearPrice() {
            return year;
        }
    }

}
