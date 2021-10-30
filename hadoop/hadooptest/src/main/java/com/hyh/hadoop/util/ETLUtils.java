package com.hyh.hadoop.util;

//数据清洗工具类
public class ETLUtils {
    public static StringBuffer vedioETL(String str) {
        //将相关视频id字段分割符从\t修改为&
        String oriStr = str;
        //o4x-VW_rCSE	HollyW00d81	581	Entertainment	74	3534116	4.48	9538	7756	o4x-VW_rCSE	d2FEj5BCmmM	8kOs3J0a2aI	7ump9ir4w-I	w4lMCVUbAyA	cNt29huGNoc	1JVqsS16Hw8	ax58nnnNu2o	CFHDCz3x58M	qq-AALY0DE8	2VHU9CBNTaA	KLzMKnrBVWE	sMXQ8KC4w-Y	igecQ61MPP4	B3scImOTl7U	X1Qg9gQKEzI	7krlgBd8-J8	naKnVbWBwLQ	rmWvPbmbs8U	LMDik7Nc7PE
        String oriStrs[] = oriStr.split("\t");
        if (oriStrs.length < 9) {
            return null;
        }
        //将视频类别的空格替换为空
        oriStrs[3] = oriStrs[3].replace(" ", "");
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < oriStrs.length; i++) {
            if (i < 9) {
                stringBuffer.append(oriStrs[i] + "\t");
            } else {
                if (i == oriStrs.length - 1) {
                    stringBuffer.append(oriStrs[i]);
                } else {
                    stringBuffer.append(oriStrs[i] + "&");
                }
            }
        }
        return stringBuffer;
    }
}
