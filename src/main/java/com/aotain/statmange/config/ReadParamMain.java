package com.aotain.statmange.config;

import com.aotain.common.config.LocalConfig;
import com.aotain.common.utils.tools.FileUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;

public class ReadParamMain {

    public static void main(String[] args) {

        for (int i = 0; i < args.length; i++) {
            System.out.println("index:" + i + ",value:" + args[i]);
        }
        String key = args[0];

        ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath*:spring/spring-base.xml");

        String value = LocalConfig.getInstance().getHashValueByHashKey(key);

        System.out.println(key+":"+value);
        System.out.println(value);
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
