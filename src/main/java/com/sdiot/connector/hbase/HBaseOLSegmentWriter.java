package com.sdiot.connector.hbase;

import com.sdiot.util.HBaseUtil;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseOLSegmentWriter extends RichSinkFunction<Tuple4<String, Long, Long, Integer>> {
    private static final Logger logger = LoggerFactory.getLogger(HBaseOLSegmentWriter.class);

    private Connection connection = null;
    private Table table = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        org.apache.hadoop.conf.Configuration hconf = HBaseConfiguration.create();
        hconf.addResource(getClass().getResource("/hbase-site.xml"));
        connection = ConnectionFactory.createConnection(hconf);
        table = connection.getTable(TableName.valueOf(HBaseUtil.HBASE_OUT_TABLE));
    }

    @Override
    public void close() throws Exception {
        if (table != null) {
            table.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }

    @Override
    public void invoke(Tuple4<String, Long, Long, Integer> value, Context context) throws Exception {
        Put put = new Put(Bytes.toBytes(
                new StringBuffer(value.f0).reverse().toString()
                        + value.f1 + value.f2));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("devid"), Bytes.toBytes(value.f0));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("start"), Bytes.toBytes(value.f1.toString()));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("end"), Bytes.toBytes(value.f2.toString()));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("endStatus"), Bytes.toBytes(value.f3.toString()));

//        logger.info("Put:{}", value);
        table.put(put);
    }
}
