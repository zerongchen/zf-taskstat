package com.aotain.statmange;

import com.aotain.common.config.LocalConfig;
import com.aotain.common.utils.tools.FileUtils;
import com.aotain.statmange.service.AzkabanServiceImpl;
import com.aotain.statmange.utils.CornConfig;
import com.aotain.statmange.utils.EnvConfig;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @Author: chenym@aotain.com
 * @Date: 2018/4/19 10:43
 * @Version: 2.7.3
 */
public class Main {

    private static Logger LOG = LoggerFactory.getLogger(Main.class);

    private static AzkabanServiceImpl impl=new AzkabanServiceImpl();


    public static void main(String[] args) {

        for (int i = 0; i < args.length; i++) {
            System.out.println("index:" + i + ",value:" + args[i]);
        }

        try {
            ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath*:spring/spring-base.xml");

            // D:\province\zf-statmange\2_src\1_trunk\zf-taskstat
            String workPath = args[0];
            String projectName = args[1];
            String configPath = workPath + File.separator + "zf_task_stat" + File.separator + "conf";
            String pDirectory=workPath + File.separator + "zf_task_stat";
            String zDirectory=workPath + File.separator + "zf_task_stat_"+System.currentTimeMillis()/1000+".zip";

            String userDir = System.getProperty("user.dir");
            System.out.println("configPath:" + configPath);
            System.out.println("userDir:" + userDir);

            File configFile = new File(configPath, "config");
            String sb = getConfigContents();

            FileUtils.writeToFile(sb.getBytes(), configFile.getAbsolutePath());

            FileUtils.zipFiles(new File(pDirectory),new File(zDirectory));

            ResourceBundle config = CornConfig.getInstance(workPath).getConfig();

            boolean h = impl.releaseProject(zDirectory,projectName,config);
            if(h){
                LOG.info("start end ,status is true ");
            }else{
                LOG.info("start end ,status is false ");
            }
        } catch (Exception e) {
            LOG.error("start stat main error ", e);
        }

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            System.exit(0);
        }
    }

   private static String getConfigContents() {
        String url = EnvConfig.getInstance().getUrl();
        String username = EnvConfig.getInstance().getUsername();
        String password = EnvConfig.getInstance().getPassword();
        String DB_URL = url;
        String DB_USER = username;
        String DB_PASSWD = password;

        String ip = null;
        String regEx = "((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?)";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(url);

        if (m.find()) {
            String result = m.group();
            ip = result;
        }

        String DB_HOSTNAME = ip;
        String DB_PORT = "3306";
        String DB_DATABASE = url.substring(url.indexOf(":3306/") + 6, url.indexOf("?"));



        Map<String, String> kvs = Maps.newHashMap();
        kvs.put("DB_URL", DB_URL);
        kvs.put("DB_USER", DB_USER);
        kvs.put("DB_PASSWD", DB_PASSWD);
        kvs.put("DB_HOSTNAME", DB_HOSTNAME);
        kvs.put("DB_PORT", DB_PORT);
        kvs.put("DB_DATABASE", DB_DATABASE);
/*        kvs.put("UBAS_TRAFFIC_EXPORT_PATH", UBAS_TRAFFIC_EXPORT_PATH);
        kvs.put("UBAS_APPFLOW_EXPORT_PATH", UBAS_APPFLOW_EXPORT_PATH);
        kvs.put("RECEIVED_TIMEOUT", RECEIVED_TIMEOUT);
        kvs.put("UPLOAD_TIMEOUT", UPLOAD_TIMEOUT);
        kvs.put("DATABASE_SCHEMA", DATABASE_SCHEMA);*/
        return getConfigByKV(kvs);
    }

   private static String getConfigByKV(Map<String, String> kvs) {
        String sb = "";
        for (Map.Entry<String, String> kv : kvs.entrySet()) {
            sb += kv.getKey() + "=" + kv.getValue() + "\n";
        }
        return sb;
    }
}
