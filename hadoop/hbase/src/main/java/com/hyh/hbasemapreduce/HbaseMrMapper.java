package com.hyh.hbasemapreduce;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * 从t1表中过滤出age=27的数据写如到t2表中
 */
public class HbaseMrMapper extends TableMapper<Text, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Text text = new Text();
        Put put = new Put(key.copyBytes());
        Cell[] cells = value.rawCells();
        for (Cell cell :
                cells) {
            if (Bytes.toString(CellUtil.cloneFamily(cell)).equals("info") &&
                    Bytes.toString(CellUtil.cloneQualifier(cell)).equals("age") &&
                    Bytes.toString(CellUtil.cloneValue(cell)).equals("27")) {
                for (Cell cell1 :
                        cells) {
//                    put.addColumn(CellUtil.cloneFamily(cell1),CellUtil.cloneQualifier(
//                            cell1),CellUtil.cloneValue(cell1));
                    put.add(cell1);
                }
                text.set(Bytes.toString(CellUtil.cloneRow(cell)));
                context.write(text, put);
            }
        }
    }
}
