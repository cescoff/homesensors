package com.desi.data.zoho;

import com.desi.data.Connector;
import com.desi.data.PlatformClientId;
import com.desi.data.SensorNameProvider;
import com.desi.data.SensorRecord;
import com.desi.data.config.PlatformCredentialsConfig;
import com.desi.data.utils.JAXBUtils;
import com.google.common.base.Optional;
import org.joda.time.LocalDateTime;

import java.util.concurrent.atomic.AtomicBoolean;

public class ZohoConnector implements Connector {

    // https://creator.zoho.com/api/corentin.escoffier/json/desihomesensors/form/SensorData/record/add
    //
    // https://creator.zoho.com/api/<ownername>/<format>/<applicationName>/form/<formName>/record/add

    // https://creator.zoho.com/api/json/desihomesensors/view/SensorData_Report
    // https://creator.zoho.com/api/<format>/<applicationLinkName>/view/<viewLinkName>


    private static final String OWNER_NAME = "corentin.escoffier";
    private static final String FORMAT = "json";
    private static final String APPLICATION_NAME = "desihomesensors";
    private static final String RECORDS_FORM_NAME = "SensorData";

    private final AtomicBoolean INIT_DONE = new AtomicBoolean(false);

    private String ownerName;

    private String format;

    private String applicationName;

    private String recordFormName;

    public ZohoConnector() {
    }

    private synchronized void init() {
        if (INIT_DONE.get()) {
            return;
        }
        this.ownerName = OWNER_NAME;
        this.format = FORMAT;
        this.applicationName = APPLICATION_NAME;
        this.recordFormName = RECORDS_FORM_NAME;
    }

    @Override
    public Optional<PlatformClientId> getPlatformId() {
        return Optional.of(PlatformClientId.Zoho);
    }

    @Override
    public boolean begin(PlatformCredentialsConfig.Credentials credentials) {
        init();
        return false;
    }

    @Override
    public boolean addRecords(Iterable<SensorRecord> records, SensorNameProvider nameProvider) throws Exception {

        return false;
    }

    @Override
    public Optional<LocalDateTime> getCheckPointValue(String sensorId) {
        return null;
    }

    @Override
    public boolean end() {
        return false;
    }
}

