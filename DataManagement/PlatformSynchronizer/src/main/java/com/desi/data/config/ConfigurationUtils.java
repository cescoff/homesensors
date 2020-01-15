package com.desi.data.config;

import org.apache.commons.lang.SystemUtils;

import java.io.File;

public class ConfigurationUtils {

    public static File getStorageDir() {
        final File storageDir = new File(SystemUtils.getUserDir(), "carsensors");
        if (!storageDir.exists()) storageDir.mkdir();
        return storageDir;
    }

    public static File getAnnotationsFile() {
        return new File(getStorageDir(), "image-annotations.xml");
    }

}
