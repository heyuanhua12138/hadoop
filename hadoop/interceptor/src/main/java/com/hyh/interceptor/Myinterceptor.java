package com.hyh.interceptor;

import com.hyh.util.ETLUtil;
import com.hyh.util.EtlUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Myinterceptor implements Interceptor {
    private List<Event> eventList = new ArrayList<Event>();
    //private Logger logger = LoggerFactory.getLogger(Myinterceptor.class);

    public void initialize() {

    }

    public Event intercept(Event event) {
        //清洗数据
        //获取header数据
        Map<String, String> headers = event.getHeaders();
        //获取body数据
        byte[] body = event.getBody();
        String bodyStr = new String(body, Charset.forName("utf-8"));
        //判断是start日志还是事件日志
        if (bodyStr.contains("\"en\":\"start\"")) {
            //向header中添加key/value标识
            headers.put("topic", "start");
            //return ETLUtil.validStartLog(bodyStr) ? event : null;
            return EtlUtils.validateStartTopic(bodyStr) ? event : null;
        } else {
            headers.put("topic", "event");
            //return ETLUtil.validEventLog(bodyStr) ? event : null;
            return EtlUtils.validateEventTopic(bodyStr) ? event : null;
        }
    }

    public List<Event> intercept(List<Event> list) {
        eventList.clear();
        for (Event event : list) {
            Event e = intercept(event);
            if (e != null) {
                eventList.add(e);
            }
        }
        return eventList;
    }

    public void close() {

    }

    public static class Builder implements org.apache.flume.interceptor.Interceptor.Builder {

        public Interceptor build() {
            return new Myinterceptor();
        }

        public void configure(Context context) {

        }
    }
}
