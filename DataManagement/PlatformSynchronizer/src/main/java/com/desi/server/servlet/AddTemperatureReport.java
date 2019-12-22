package com.desi.server.servlet;

import com.google.common.collect.Sets;
import org.apache.commons.io.FileSystemUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class AddTemperatureReport extends AbstractReportServlet {

    private static final Logger logger = LoggerFactory.getLogger(AddTemperatureReport.class);

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    @Override
    public String performAction(Map<String, String> parameters, File outputDir) {
        final String sensorId = parameters.get("s");
        final String valueStr = parameters.get("v");
        final String folder = parameters.get("f");


        final String csvLine = new StringBuilder().
                append(sensorId).append(";").
                append(LocalDateTime.now().format(DATE_FORMATTER)).append(";").
                append(LocalDateTime.now().format(TIME_FORMATTER)).append(";").
                append("C=").append(valueStr).toString();

        logger.info("Writting csv '" + csvLine + "'");

        final FileOutputStream fileOutputStream;
        try {
            fileOutputStream = new FileOutputStream(new File(outputDir, "temperatures.log"), true);
        } catch (FileNotFoundException e) {
            logger.error("Failed to open '" + new File(outputDir, "temperatures.log").getPath() + "'", e);
            return "ERROR";
        }
        try {
            fileOutputStream.write(new StringBuilder(csvLine).append("\n").toString().getBytes());
        } catch (IOException e) {
            logger.error("Failed to write into file '" + new File(outputDir, "temperatures.log").getPath() + "'", e);
            try {
                fileOutputStream.close();
            } catch (IOException ex) {
            }
            return "ERROR";
        }
        try {
            fileOutputStream.close();
        } catch (IOException e) {
            logger.error("Failed to close file '" + new File(outputDir, "temperatures.log").getPath() + "'", e);
            return "ERROR";
        }

        return "DONE";
    }

    @Override
    public String getFolder(Map<String, String> parameters) {
        return parameters.get("f");
    }
}
