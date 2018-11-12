package com.aotain.statmange.kafkamessage;

import com.aotain.common.config.LocalConfig;
import com.aotain.common.utils.kafka.KafkaProducer;
import com.aotain.common.utils.tools.CommonConstant;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaMessageMain {
    private static SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
    protected static Map<String, Object> conf = new HashMap<String, Object>();

    public static void main(String[] args) {

        for(int i=0;i<args.length;i++){
            System.out.println("index:"+i+",args value:"+args[i]);
        }
        String filetype= null;
        String filename= null;
        String filetime= null;
        String filesize= null;
        String filerecord= null;
        try {
            filetype = args[0];
            filename = args[1];
            filetime = args[2];
            filesize = args[3];
            filerecord = args[4];
        } catch (Exception e) {
            System.out.println("parse arguments error "+e);
            System.exit(12);
        }

        printArgs(filetype,filename,filetime,filesize,filerecord);
        Map<String, String> map0 = new HashMap<String, String>();
        map0.put("filetype", Integer.parseInt(filetype,16)+"");
        map0.put("filename", filename);

        try {
            map0.put("filetime", (format.parse(filetime)).getTime()/1000+"");
        } catch (ParseException e) {
            System.out.println("parse date error "+e);
            System.exit(12);
        }

        map0.put("filesize", filesize);
/*        if(!StringUtils.isEmpty(filesize)){
            BigDecimal bg = new BigDecimal(Double.valueOf(filesize)/1024);
            double f1 = bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
            map0.put("filesize", f1+"");
        }*/
        map0.put("filerecord", filerecord+"");

        Map<String, Object> message_map = new HashMap<String, Object>();
        message_map.put("datatype", 3);
        message_map.put("datasubtype", 301);
        message_map.put("datamessage", map0);


        Map<String,Object> smmsMessage = new HashMap<String,Object>();
        smmsMessage.put("type", "4");
        smmsMessage.put("createtime", System.currentTimeMillis()/1000);
        smmsMessage.put("message", message_map);

        try {
            smmsMessage.put("createip", InetAddress.getLocalHost().getHostAddress().toString());
        } catch (UnknownHostException e) {
            System.out.println("get ip error "+e);
            e.printStackTrace();
            System.exit(12);
        }

        try {
            ObjectMapper json = new ObjectMapper();
            String message = json.writeValueAsString(smmsMessage);
            System.out.println("message:"+message);

            ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath*:spring/spring-base.xml");

            Map<String, Object> conf = LocalConfig.getInstance().getKafkaProducerConf();
            System.out.println("conf:"+conf);
            KafkaProducer producer = new KafkaProducer(conf);
            System.out.println("producer:"+producer);
            boolean b = producer.producer(CommonConstant.KAFKA_QUEUE_NAME_NOTICE, message);
            System.out.println("after the file export task is written kafka success. msg: "+message);
            System.out.println("====== kafka message main end ========");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(" send message to kafka error,"+e);
            System.exit(12);
        }finally{
            System.exit(0);
        }

    }

    /**
     * @param filetype
     * @param filename
     * @param filetime
     * @param filesize
     * @param filerecord
     */
    public static void printArgs(String filetype, String filename, String filetime, String filesize,String filerecord) {
        System.out.println("[ExportCompleteMessageService].[args:  filetype]:" + filetype);
        System.out.println("[ExportCompleteMessageService].[args:  filename]:" + filename);
        System.out.println("[ExportCompleteMessageService].[args:  filetime]:" + filetime);
        System.out.println("[ExportCompleteMessageService].[args:  filesize]:" + filesize);
        System.out.println("[ExportCompleteMessageService].[args:filerecord]:" + filerecord);
    }

}
