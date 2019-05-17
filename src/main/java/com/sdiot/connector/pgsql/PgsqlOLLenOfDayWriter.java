package com.sdiot.connector.pgsql;

import com.sdiot.util.SqlConstants;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class PgsqlOLLenOfDayWriter extends RichSinkFunction<Tuple3<String, String, Long>> {
    private static final Logger logger = LoggerFactory.getLogger(PgsqlOLLenOfDayWriter.class);

    private Connection connection = null;
    private CallableStatement callableStatement = null;
//    private Statement statement = null;

//    private static final String dbUrl = "jdbc:postgresql://192.168.10.22:5432/iot";
//    private static final String dbUser = "postgres";
//    private static final String dbPwd = "postgres";
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.postgresql.Driver");

        connection = DriverManager.getConnection(SqlConstants.DB_URL, SqlConstants.DB_USER, SqlConstants.DB_PWD);


        Statement statement = connection.createStatement();
        statement.execute(SqlConstants.IU_OL_LENGTH_OF_DAY);
        statement.close();


        callableStatement = connection.prepareCall("{call f_iu_ol_length_of_day(?, ?, ?, ?)}");
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {

//        if (statement != null){
//            statement.close();
//        }
        if(callableStatement != null){
            callableStatement.close();
        }
        if(connection != null){
            connection.close();
        }

        super.close();
    }

    @Override
    public void invoke(Tuple3<String, String, Long> value, Context context) throws Exception {
        try {
            callableStatement.setString(1, value.f0);
            callableStatement.setDate(2, Date.valueOf(value.f1));
            callableStatement.setLong(3, value.f2);
            callableStatement.setTimestamp(4, new Timestamp(new java.util.Date().getTime()));
            callableStatement.execute();
        } catch (Exception e){
            logger.error(">>>>>>>>>>exception:", e);
        }
    }
}
