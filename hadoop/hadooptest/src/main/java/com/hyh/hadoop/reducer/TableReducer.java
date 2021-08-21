package com.hyh.hadoop.reducer;

import com.hyh.hadoop.bean.TableBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.util.BeanUtil;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    String pName;

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //1001	01	1
        //1004	01	4
        //01	小米
        List<TableBean> tableBeanList = new ArrayList<>();
        for (TableBean tableBean : values) {
            TableBean tb = new TableBean();
            try {
                BeanUtils.copyProperties(tb,tableBean);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
            if (tb.getTableFlag().equals("order")) {
                //订单

                tableBeanList.add(tb);
            } else {
                pName = tb.getpName();
            }
        }
        for (TableBean tableBean : tableBeanList) {
            tableBean.setpName(pName);
            context.write(tableBean,NullWritable.get());
        }
        tableBeanList.clear();
    }
}
