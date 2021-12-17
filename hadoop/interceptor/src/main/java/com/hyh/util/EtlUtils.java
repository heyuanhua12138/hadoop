package com.hyh.util;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EtlUtils {
    //private static Logger logger = LoggerFactory.getLogger(EtlUtils.class);
    public static boolean validateStartTopic(String source) {

        if (StringUtils.isBlank(source)) {
            return false;
        }
        /*if(!source.trim().matches("^\\{.+}$")){
            logger.info("source====="+source);
        }else{
            logger.info("source_normal====="+source);
        }*/
        return source.trim().matches("^\\{.+}$");
    }

    public static boolean validateEventTopic(String source) {
        if (StringUtils.isBlank(source)) {
            return false;
        }
        return source.trim().matches("\\d{13}\\|\\{.+}");
    }
}
