package com.desi.server.servlet;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;

public abstract class AbstractReportServlet extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(AbstractReportServlet.class);

    private Set<String> OUTDIR_PRINTED = Sets.newHashSet();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        final ImmutableMap.Builder<String, String> params = ImmutableMap.builder();
        final Enumeration<String> parameterNames = req.getParameterNames();
        while (parameterNames.hasMoreElements()) {
            final String parameterName = parameterNames.nextElement();
            params.put(parameterName, req.getParameter(parameterName));
        }
        final String output = performAction(params.build(), getOutputDir(getFolder(params.build())));
        resp.getOutputStream().write(output.getBytes());
    }

    public abstract String performAction(final Map<String, String> parameters, final File outputDir);

    public  abstract String getFolder(final Map<String, String> parameters);

    private File getOutputDir(final String folder) {
        final String realFolder;
        if (StringUtils.isEmpty(folder)) {
            realFolder = "default";
        } else {
            realFolder = folder;
        }
        final String outputDirStr = System.getProperty("sensors.output.folder");
        if (StringUtils.isNotEmpty(outputDirStr)) {
            final File outputDir = new File(outputDirStr);
            if (!outputDir.exists() || !outputDir.isDirectory()) {
                logger.error("Directory '" + outputDirStr + "' does not exist or is not a directory, using default value instead");
            } else {
                final File result = new File(outputDir, realFolder);
                if (!result.exists()) {
                    result.mkdir();
                }
                if (!OUTDIR_PRINTED.contains(realFolder)) {
                    logger.info("Creating output folder '" + result.getPath() + "'");
                    OUTDIR_PRINTED.add(realFolder);
                }
                return result;
            }
        }
        final File result =  new File(SystemUtils.getUserHome(), realFolder);
        if (!result.exists()) {
            result.mkdir();
        }
        if (!OUTDIR_PRINTED.contains(realFolder)) {
            logger.info("Creating output folder '" + result.getPath() + "'");
            OUTDIR_PRINTED.add(realFolder);
        }
        return result;
    }

}
