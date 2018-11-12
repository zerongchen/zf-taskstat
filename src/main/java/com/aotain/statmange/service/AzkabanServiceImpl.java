package com.aotain.statmange.service;

import com.alibaba.fastjson.JSON;
import com.aotain.common.utils.azkaban.AzkabanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * @Author: chenym@aotain.com
 * @Date: 2018/4/19 14:18
 * @Version: 2.7.3
 */
@Service
public class AzkabanServiceImpl {

    private static Logger LOG = LoggerFactory.getLogger(AzkabanServiceImpl.class);

    private Map loginMap = null;
    private Map uploadReturn = null;
    private Map schedulerRetrun = null;
    private Map createPReturn = null;

    private String projectName=null;

    private AzkabanUtils azkabanUtils = null;

    public AzkabanServiceImpl loginAzkaban() {
        try {
            azkabanUtils = new AzkabanUtils();
            String loginReturn = azkabanUtils.loginAzkaban();
            loginMap = (Map) JSON.parse(loginReturn);
            LOG.info("login azkaban success !");
            return this;
        } catch (Exception e) {
            LOG.error("login azkaban,error:", e);
        }
        return null;
    }

    public AzkabanServiceImpl createAzProject(String projectName){
        try {
            this.projectName=projectName;
            String sessionId = (String) loginMap.get("session.id");
            String createPReturns = azkabanUtils.createAzProject(azkabanUtils,sessionId,projectName,projectName);
            createPReturn = (Map) JSON.parse(createPReturns);
            LOG.info("create azkaban project success !");
            return this;
        } catch (Exception e) {
            LOG.error("create project error:", e);
        }

        return null;
    }


    public AzkabanServiceImpl uploadZip(String zipPath){
        try {
            String sessionId = (String) loginMap.get("session.id");
            String uploadReturns = azkabanUtils.uploadZip(zipPath, sessionId, projectName);
            uploadReturn = (Map) JSON.parse(uploadReturns);
            LOG.info("upload azkaban project success !");
            return this;
        } catch (Exception e) {
            LOG.error("upload azkaban,error:", e);
        }
        return null;
    }
    public  String scheduleByCronEXEaFlow(String flowName,String corn){
        try {
            String sessionId = (String) loginMap.get("session.id");
            String schedulerRetruns = azkabanUtils.scheduleByCronEXEaFlow(sessionId,projectName,corn,flowName);
            schedulerRetrun = (Map) JSON.parse(schedulerRetruns);
        } catch (Exception e) {
            LOG.error("scheduler azkaban,error:", e);
        }
        return null;
    }


    public boolean releaseProject(String zipFile,String projectName,ResourceBundle config){
        try {
            AzkabanServiceImpl impl = loginAzkaban().loginAzkaban().createAzProject(projectName).uploadZip(zipFile);

            for(String flowName:config.keySet()){
                String corn=config.getString(flowName);
                impl.scheduleByCronEXEaFlow(flowName,corn);
                LOG.info("corn:"+flowName+" -> "+corn);
            }
            LOG.info("release azkaban project success !");
            return true;
        } catch (Exception e) {
            LOG.error("release project error:", e);
            return false;
        }
    }


    public Map getLoginMap() {
        return loginMap;
    }

    public Map getUploadReturn() {
        return uploadReturn;
    }
}
