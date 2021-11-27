package com.hyh.hbasemapreduce;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

public class HbaseMrReducer extends TableReducer<Text, Put, Text> {
}
