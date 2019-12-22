package com.desi.server.servlet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class PingReport extends AbstractReportServlet {

    private static final Logger logger = LoggerFactory.getLogger(PingReport.class);

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    @Override
    public String performAction(Map<String, String> parameters, File outputDir) {
        final FileOutputStream fileOutputStream;
        try {
            fileOutputStream = new FileOutputStream(new File(outputDir.getParent(), getFolder(parameters) + "_ping.log"), true);
        } catch (FileNotFoundException e) {
            logger.error("Failed to open '" + new File(outputDir, "temperatures.log").getPath() + "'", e);
            return "ERROR";
        }
        try {
            fileOutputStream.write(new StringBuilder(new StringBuilder().append(LocalDateTime.now().format(DATE_FORMATTER))).append(" ").append(LocalDateTime.now().format(TIME_FORMATTER)).append("\n").toString().getBytes());
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
