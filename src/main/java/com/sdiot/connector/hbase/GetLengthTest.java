package com.sdiot.connector.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Iterator;

public class GetLengthTest {
    public static void main(String[] args) throws Exception{
        Logger logger = LoggerFactory.getLogger("GetLengthTest.main");
        String devid = "001602136480"; //084631206100
        long start = 0L, end = new java.util.Date().getTime();
        long ol_len = 0L;

        org.apache.hadoop.conf.Configuration hconf = HBaseConfiguration.create();
        hconf.addResource(GetLengthTest.class.getResource("/hbase-site.xml"));
        Connection connection = ConnectionFactory.createConnection(hconf);
        Table table = connection.getTable(TableName.valueOf("iot_dev_ol_seg"));
//        Table table = connection.getTable(TableName.valueOf("htable_historical_track"));

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("f"))
                .setRowPrefixFilter(Bytes.toBytes(new StringBuffer(devid).reverse().toString()))
                ;
        try {
            ResultScanner resultScanner = table.getScanner(scan);
            Iterator<Result> iterator = resultScanner.iterator();
            long number = 0L;

            long lastTs = 0L;
            int lastEndStatus = 0;

            while (iterator.hasNext()) {
                Result result = iterator.next();

                String rowKey = Bytes.toString(result.getRow());
                long r_start = Long.parseLong(rowKey.substring(12, 12 + 13));
                long r_end = Long.parseLong(rowKey.substring(12 + 13, 12 + 26));

                if (r_end < start)
                    continue;
                if (end < r_start)
                    break;

                boolean startin = start <= r_start && r_start < end;
                boolean endin = start <= r_end && r_end < end;

                if (startin && endin) {
                    logger.info(">>>>startin && endin:{}<<<<", rowKey);
                    ol_len += r_end - r_start;

                    if (lastEndStatus == 1 && lastTs!=0){
                        ol_len += r_start - lastTs;
                    }

                    number++;
                } else if (startin) {
                    logger.info(">>>>startin:{}<<<<", rowKey);
                    ol_len += end - r_start;

                    if (lastEndStatus == 1 && lastTs != 0){
                        ol_len += r_start - lastTs;
                    }

                    number++;
                } else if (endin) {
                    logger.info(">>>>endin:{}<<<<", rowKey);
                    ol_len += r_end - start;
                    number++;
                }

                lastTs = r_end;

                for (Cell cell : result.listCells()) {
                    String qualifier = Bytes.toString(cell.getQualifierArray(),
                            cell.getQualifierOffset(), cell.getQualifierLength());

                    if (qualifier.equalsIgnoreCase("endStatus")) {

                        String value = Bytes.toString(cell.getValueArray(),
                                cell.getValueOffset(), cell.getValueLength());

                        try{
                            lastEndStatus = Integer.parseInt(value);
                        }catch (Exception e){
                            lastEndStatus = 0;
                        }

                        break;
                    }
                }
            }

            logger.info(">>>>>>>>online length={},{}, numbers of row = {}", ol_len, Duration.ofMillis(ol_len).toString(), number);
        } catch (Exception e){
            logger.info("{}", e);
        }
        table.close();
        connection.close();
    }
}
