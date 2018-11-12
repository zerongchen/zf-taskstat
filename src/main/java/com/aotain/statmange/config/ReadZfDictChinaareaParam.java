package com.aotain.statmange.config;

import com.aotain.common.config.ContextUtil;
import com.aotain.statmange.service.ZfDictChinaareaServiceImpl;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.HashMap;
import java.util.Map;

public class ReadZfDictChinaareaParam {

    public static void main(String[] args) {
        for (int i = 0; i < args.length; i++) {
            System.out.println("index:" + i + ",value:" + args[i]);
        }
        String key = args[0];

        ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath*:spring-base-statmanage.xml");

        ZfDictChinaareaServiceImpl zfDictChinaareaServiceImpl = ContextUtil.getContext().getBean("zfDictChinaareaServiceImpl",ZfDictChinaareaServiceImpl.class);
        Map<String, Object> map = new HashMap<>();
        map.put("area_code",key);
        String area_short = zfDictChinaareaServiceImpl.select_zf_dict_chinaarea(map);
        System.out.println(area_short);

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
