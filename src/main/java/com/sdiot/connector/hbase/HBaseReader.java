package com.sdiot.connector.hbase;

import com.sdiot.util.HBaseUtil;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;


public class HBaseReader extends RichSourceFunction<Tuple3<String, Long, Integer>> {
    private static final Logger logger = LoggerFactory.getLogger(HBaseReader.class);

    private Connection connection = null;
    private Table table = null;
    private Scan scan = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //
        org.apache.hadoop.conf.Configuration hconf = HBaseConfiguration.create();
        hconf.addResource(getClass().getResource("/hbase-site.xml"));
        connection = ConnectionFactory.createConnection(hconf);
        table = connection.getTable(TableName.valueOf(HBaseUtil.HBASE_IN_TABLE));

        scan = new Scan();
        // Set date section
        Calendar calendar = Calendar.getInstance();
//        calendar.add(Calendar.MONTH, -1);
//        calendar.set(2018, 10, 1);

        //
        scan.addFamily(Bytes.toBytes("f"))
//                .setStartRow(Bytes.toBytes(""))
//                .setStopRow(Bytes.toBytes(""))
//                .setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("\\d+P\\d+")))
//                .setTimeRange(calendar.getTimeInMillis(),new Date().getTime())
        ;

    }

    @Override
    public void run(SourceContext<Tuple3<String, Long, Integer>> sourceContext) throws Exception {
        ResultScanner resultScanner = table.getScanner(scan);
        Iterator<Result> iterator = resultScanner.iterator();

        while (iterator.hasNext()) {
            Result result = iterator.next();

            String rowKey = Bytes.toString(result.getRow());

            if(rowKey.length() != 12+11+7+13)
                continue;

            int online = -1;
            long ts = -1;
            for (Cell cell : result.listCells()) {
                String qualifier = Bytes.toString(cell.getQualifierArray(),
                        cell.getQualifierOffset(), cell.getQualifierLength());

                if (qualifier.equalsIgnoreCase("onLine")) {

                    String value = Bytes.toString(cell.getValueArray(),
                            cell.getValueOffset(), cell.getValueLength());
                    ts = cell.getTimestamp();

                    try{
                        online = Integer.parseInt(value);
                    }catch (Exception e){
//                        continue;
                    }
                    if(online != 1){
//                        logger.info("{} =====> [{}] OFFLINE!", rowKey, online);
                        online = 0;
                    }
                    break;
                }
            }
            if (online != -1) {
                Tuple3<String, Long, Integer> tuple3 = new Tuple3<>();
                // cell的时间戳与rowkey里带的时间戳是不一致的，rowkey的时间戳是取整的
                String rev = new StringBuffer(rowKey.substring(0, 12)).reverse().toString();
                rev += rowKey.substring(12, 30);
                tuple3.setFields(rev, ts, online);

//                logger.info("<<<<<{}>>>>>", tuple3);

                sourceContext.collect(tuple3);
            }
        }
    }

    @Override
    public void cancel() {
        try {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            logger.error("Close HBase Exception:", e.toString());
        }
    }
}
