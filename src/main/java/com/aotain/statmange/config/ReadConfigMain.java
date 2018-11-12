package com.aotain.statmange.config;

import com.aotain.common.config.LocalConfig;
import com.aotain.common.utils.tools.FileUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;

public class ReadConfigMain {

    public static void main(String[] args) {

        for (int i = 0; i < args.length; i++) {
            System.out.println("index:" + i + ",value:" + args[i]);
        }
        String workPath = args[0];
        File configFile= new File(workPath,"trafficConfig");

        ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath*:spring/spring-base.xml");

        String deployProvinceShortName = LocalConfig.getInstance().getHashValueByHashKey("system.deploy.province.shortname");
        String depolyProvinceProvince = LocalConfig.getInstance().getHashValueByHashKey("system.deploy.province.provider");

        System.out.println("deployProvinceShortName:"+deployProvinceShortName);
        System.out.println("depolyProvinceProvince:"+depolyProvinceProvince);

        String configStr = "deployProvinceShortName="+deployProvinceShortName+"\n"+"depolyProvinceProvince="+depolyProvinceProvince;

        try {
            FileUtils.writeToFile(configStr.getBytes(), configFile.getAbsolutePath());

            System.out.println("配置文件已经生成,生成路径:"+configFile.getAbsolutePath());
        } catch (Exception e) {
            System.err.println(e);
        }
        System.out.println(configFile.getAbsolutePath());

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            System.exit(0);
        }
    }

}
