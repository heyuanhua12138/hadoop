package com.hyh.hbase.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 命名空间工具类
 * 负责NameSpace的增删查改
 */
public class NameSpaceUtil {
    private static Logger LOGGER = LoggerFactory.getLogger(NameSpaceUtil.class);

    /**
     * 查找所有的namespace
     */
    public static List<String> ListNameSpaces(Connection connection) throws IOException {
        //通过Admin对象操作namespace命名空间
        List<String> list = new ArrayList<String>();
        Admin admin = connection.getAdmin();
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor :
                namespaceDescriptors) {
            list.add(namespaceDescriptor.getName());
        }
        //使用完毕，关闭admin
        admin.close();
        return list;
    }

    /**
     * 判断命名空间是否存在
     *
     * @param connection
     * @param name
     * @return
     * @throws IOException
     */
    public static boolean existsNameSpace(Connection connection, String name) throws IOException {
        if (StringUtils.isEmpty(name)) {
            LOGGER.error("请输入正确的namespace名称");
            return false;
        }
        Admin admin = connection.getAdmin();
        try {
            admin.getNamespaceDescriptor(name);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            admin.close();
        }
    }

    /**
     * 创建NameSpace
     * @param connection
     * @param name
     * @throws IOException
     */
    public static void createNameSpace(Connection connection, String name) throws IOException {
        if (StringUtils.isEmpty(name)) {
            LOGGER.error("请输入正确的namespace名称");
            return;
        }
        Admin admin = connection.getAdmin();
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(name).build();
        admin.createNamespace(namespaceDescriptor);
        admin.close();
    }

    /**
     * 删除NameSpace
     * @param connection
     * @param name
     * @throws IOException
     */
    public static void deleteNameSpace(Connection connection,String name) throws IOException {
        if (StringUtils.isEmpty(name)) {
            LOGGER.error("请输入正确的namespace名称");
            return;
        }
        Admin admin = connection.getAdmin();
        admin.deleteNamespace(name);
        admin.close();
    }

}
