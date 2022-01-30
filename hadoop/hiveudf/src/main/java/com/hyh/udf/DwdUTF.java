package com.hyh.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DwdUTF extends UDF {
    /**
     * 编写UDF函数（1进1出），a
     * ①取ap属性的值app:   a(jsonstr,'ap')=app
     * ②取json的1581472611770：   a(jsonstr,'ts')=1581472611770
     * ③取cm公共部分中的属性，例如：
     * 取cm中的ln的值：  a(jsonstr,'ln')=-45.5
     * 取cm中的sv的值：  a(jsonstr,'sv')=V2.7.9
     *
     * @param source
     * @return
     */
    private Logger logger = LoggerFactory.getLogger(DwdUTF.class);
    public String evaluate(String source, String param) throws JSONException {
        //判断param是否在源JSON字符串中
        if (!source.contains(param) && !"ts".equals(param)) {
            return "";
        }
        String[] strs = source.split("\\|");
        JSONObject jsonObject = new JSONObject(strs[1]);
        if ("ts".equals(param)) {
            return strs[0];
        } else if ("ap".equals(param)) {
            return jsonObject.getString("ap");
        } else if ("et".equals(param)) {
            return jsonObject.getString("et");
        } else {
            return jsonObject.getJSONObject("cm").getString(param);
        }
    }
}
