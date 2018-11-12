package com.aotain.statmange.kafkamessage;

import com.alibaba.fastjson.JSON;
import com.aotain.common.config.LocalConfig;
import com.aotain.common.utils.kafka.KafkaProducer;
import com.aotain.common.utils.tools.CommonConstant;
import org.apache.commons.lang.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.net.InetAddress;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WriteWarnMsgToKafkaMain {


    public static void main(String[] args){

        Integer dataType =null;
        String content=null;
        List<String> list = new ArrayList<String>();
        if (args.length<2){
            System.out.println("there is no data push to kafka");
            System.exit(0);
        }
        try {
            dataType = Integer.parseInt(args[0]);
            content = args[1];
            System.out.println(dataType+"----"+content);
            String[] contents = content.split("\\|");
            if (contents.length<1) {
                System.out.println("there is no data push to kafka");
                System.exit(0);
            }
            for (int i=0;i<contents.length;i++){
                Integer fileType = Integer.parseInt(contents[i]);
                String timeTsamp =contents[++i];
                Long warnNum = Long.parseLong(contents[++i]);
                String formt = getFileDesc(dataType,fileType);
                if (!StringUtils.isEmpty(formt)){
                    list.add(timeTsamp+String.format(formt,warnNum));
                }
            }
        }catch (Exception e){
            System.out.println("parse arguments error "+e);
            System.exit(12);
        }
        List<String> mapList = new ArrayList<String>();
        for (String msg:list){
            Map<String,Object> smmsMessage = new HashMap<String,Object>();
            Map<String ,String > msgMap = new HashMap<>();
            msgMap.put("content",msg);
            try {
                smmsMessage.put("type", "5");
                smmsMessage.put("createtime", System.currentTimeMillis()/1000);
                smmsMessage.put("message",msgMap);
                smmsMessage.put("createip", InetAddress.getLocalHost().getHostAddress().toString());
            } catch (UnknownHostException e) {
                System.out.println("get ip error "+e);
                e.printStackTrace();
                System.exit(12);
            }
            System.out.println("send warn msg to kafka. msg: "+JSON.toJSONString(smmsMessage));
            mapList.add(JSON.toJSONString(smmsMessage));
            }
        try {
            ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath*:spring/spring-base.xml");

            Map<String, Object> conf = LocalConfig.getInstance().getKafkaProducerConf();
            System.out.println("conf:"+conf);
            KafkaProducer producer = new KafkaProducer(conf);
            System.out.println("producer:"+producer);
            boolean b = producer.producer(CommonConstant.KAFKA_QUEUE_NAME_NOTICE, mapList);
            System.out.println("====== kafka message main end ========");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(" send message to kafka error,"+e);
            System.exit(12);
        }finally{
            System.exit(0);
        }
    }

    private static String getFileDesc(Integer dataType,Integer fileType){

        String dataDec;

        if(dataType==1){
            dataDec=" 生成";
        }else if(dataType==2){
            dataDec=" 接收";
        }else{
            dataDec=" 上报";
        }
        //0x03ff=AAA生成文件信息,0x0102=全业务流量生成文件信息,0x01c4=业务流量流向生成文件信息,0x0300=HTTPGET文件
        if(fileType==0x03ff){
            return dataDec+"AAA数据有 %s 个异常文件";
        }else if(fileType==0x0102){
            return dataDec+"全业务流量数据有 %s 个异常文件";
        }else if(fileType==0x01c4){
            return dataDec+"应用流量流向数据有 %s 个异常文件";
        }else if(fileType==0x0300){
            return dataDec+"HTTPGET文件数据有 %s 个异常文件";
        }
        return null;
    }


}
