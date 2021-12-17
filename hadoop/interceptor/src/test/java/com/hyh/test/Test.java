package com.hyh.test;

import com.hyh.interceptor.Myinterceptor;
import com.hyh.util.ETLUtil;
import com.hyh.util.EtlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Test {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Myinterceptor.class);
        String str = "{\"action\":\"2\",\"ar\":\"MX\",\"ba\":\"Sumsung\",\"detail\":\"\",\"en\":\"start\",\"entry\":\"5\",\"extend1\":\"\",\"g\":\"EHH95FVY@gmail.com\",\"hw\":\"640*960\",\"l\":\"en\",\"la\":\"-30.7\",\"ln\":\"-105.8\",\"loading_time\":\"1\",\"md\":\"sumsung-5\",\"mid\":\"1\",\"nw\":\"3G\",\"open_ad_type\":\"1\",\"os\":\"8.2.4\",\"sr\":\"T\",\"sv\":\"V2.7.2\",\"t\":\"1639218518620\",\"uid\":\"1\",\"vc\":\"17\",\"vn\":\"1.0.2\"}";
        logger.error("str"+str);
        //System.out.println(EtlUtils.validateStartTopic(str));
        System.out.println(ETLUtil.validStartLog(str));
        String str1 = "1639242133348|{\"cm\":{\"ln\":\"-74.8\",\"sv\":\"V2.6.5\",\"os\":\"8.1.6\",\"g\":\"40DFZV9I@gmail.com\",\"mid\":\"0\",\"nw\":\"4G\",\"l\":\"en\",\"vc\":\"19\",\"hw\":\"640*960\",\"ar\":\"MX\",\"uid\":\"0\",\"t\":\"1639211317587\",\"la\":\"-52.4\",\"md\":\"HTC-5\",\"vn\":\"1.2.9\",\"ba\":\"HTC\",\"sr\":\"L\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1639146943127\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"12\",\"action\":\"3\",\"extend1\":\"\",\"type\":\"1\",\"type1\":\"542\",\"loading_way\":\"2\"}},{\"ett\":\"1639227183160\",\"en\":\"ad\",\"kv\":{\"entry\":\"3\",\"show_style\":\"4\",\"action\":\"1\",\"detail\":\"\",\"source\":\"1\",\"behavior\":\"1\",\"content\":\"2\",\"newstype\":\"8\"}},{\"ett\":\"1639231072702\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1639183815046\",\"action\":\"1\",\"type\":\"2\",\"content\":\"\"}},{\"ett\":\"1639223318252\",\"en\":\"error\",\"kv\":{\"errorDetail\":\"at cn.lift.dfdfdf.control.CommandUtil.getInfo(CommandUtil.java:67)\\\\n at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\\\n at java.lang.reflect.Method.invoke(Method.java:606)\\\\n\",\"errorBrief\":\"at cn.lift.appIn.control.CommandUtil.getInfo(CommandUtil.java:67)\"}},{\"ett\":\"1639187052990\",\"en\":\"praise\",\"kv\":{\"target_id\":7,\"id\":9,\"type\":1,\"add_time\":\"1639174645838\",\"userid\":6}}]}";
        //System.out.println(EtlUtils.validateEventTopic(str1));
        System.out.println(ETLUtil.validEventLog(str1));
    }


}
