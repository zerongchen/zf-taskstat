package com.aotain.statmange.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

public class CornConfig {

    private static Logger LOG = LoggerFactory.getLogger(CornConfig.class);

    private static CornConfig instance;

    private ResourceBundle config;

    private CornConfig(String workPath) {

        String proFilePath = "";
        try {
            if (System.getProperty("os.name").toLowerCase().contains("windows")) {
                proFilePath = this.getClass().getResource("/").getPath() + File.separator + "corn.properties";
            } else if (System.getProperty("os.name").toLowerCase().contains("linux")) {
                proFilePath = workPath + File.separator + "config" + File.separator + "corn.properties";
            }
        } catch (Exception e) {
            LOG.error("", e);
        }
        InputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(proFilePath));
        } catch (FileNotFoundException e) {
            LOG.error("", e);
        }
        try {
            config = new PropertyResourceBundle(in);
        } catch (IOException e) {
            LOG.error("", e);
        }
        try {
            in.close();
        } catch (IOException e) {
            LOG.error("", e);
        }

    }

    public synchronized static CornConfig getInstance(String cornFilePath) {

        if (instance == null) {
            instance = new CornConfig(cornFilePath);
        }
        return instance;
    }

    public ResourceBundle getConfig() {
        return config;
    }

    public void setConfig(ResourceBundle config) {
        this.config = config;
    }
}
