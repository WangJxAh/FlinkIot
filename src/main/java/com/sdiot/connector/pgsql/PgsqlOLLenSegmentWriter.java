package com.sdiot.connector.pgsql;

import com.sdiot.util.SqlConstants;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class PgsqlOLLenSegmentWriter extends RichSinkFunction<Tuple3<String, Long, Long>> {
    private static final Logger logger = LoggerFactory.getLogger(PgsqlOLLenSegmentWriter.class);

    private Connection connection = null;
    private CallableStatement callableStatement = null;

//    private PreparedStatement preparedStatement = null;
//
//    private static final String strSql =
//            "INSERT INTO stat_dev_ol_length_segment(devid,ol_start,ol_end,last_update_time) VALUES (?, ?,?,?);";

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.postgresql.Driver");

        connection = DriverManager.getConnection(SqlConstants.DB_URL, SqlConstants.DB_USER, SqlConstants.DB_PWD);
//        preparedStatement = connection.prepareStatement(strSql);
        Statement statement = connection.createStatement();
        statement.execute(SqlConstants.IU_OL_LENGTH_SEGMENT);
        statement.close();

        callableStatement = connection.prepareCall("{call f_iu_ol_length_segment(?, ?, ?, ?)}");
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
//        if(preparedStatement != null){
//            preparedStatement.close();
//        }
        if (callableStatement != null){
            callableStatement.close();
        }
        if(connection != null){
            connection.close();
        }

        super.close();
    }

    @Override
    public void invoke(Tuple3<String, Long, Long> value, Context context) throws Exception {
//        preparedStatement.setString(1, value.f0);
//        preparedStatement.setTimestamp(2, new Timestamp(value.f1));
//        preparedStatement.setTimestamp(3, new Timestamp(value.f2));
//        preparedStatement.setTimestamp(4, new Timestamp(new java.util.Date().getTime()));
//
//        preparedStatement.executeUpdate();

        try{
//            logger.info(">>>>>>>>>>>>>>{}=[{}, {}]<<<<<<<<<<<<<", value, new Timestamp(value.f1), new Timestamp(value.f2));

            callableStatement.setString(1, value.f0);
            callableStatement.setTimestamp(2, new Timestamp(value.f1));
            callableStatement.setTimestamp(3, new Timestamp(value.f2));
            callableStatement.setTimestamp(4, new Timestamp(new java.util.Date().getTime()));

            callableStatement.execute();
        }catch (Exception e){
            logger.error(">>>>>>>>>>{}", e);
        }
    }
}
