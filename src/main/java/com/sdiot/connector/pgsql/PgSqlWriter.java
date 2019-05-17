package com.sdiot.connector.pgsql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDate;

public class PgSqlWriter extends RichSinkFunction<Tuple2<String,String>> {
    private static final Logger logger = LoggerFactory.getLogger(PgSqlWriter.class);

    private Connection connection = null;
    private PreparedStatement preparedStatement = null;

    private static final String dbUrl = "jdbc:postgresql://192.168.10.22:5432/iot";
    private static final String dbUser = "postgres";
    private static final String dbPwd = "postgres";
    private static final String strSql = "";

    @Override
    public void open(Configuration parameters) throws Exception {

        connection = DriverManager.getConnection(dbUrl, dbUser, dbPwd);
        preparedStatement = connection.prepareStatement(strSql);

        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }

        super.close();
    }

    @Override
    public void invoke(Tuple2<String, String> value, Context context) throws Exception {
        try {
            String name = value.f0;//获取JdbcReader发送过来的结果
            preparedStatement.setString(1,name);

            // set update time
            LocalDate localDate = LocalDate.now();
            preparedStatement.setObject(1, localDate);
            //
            preparedStatement.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws SQLException {
        Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPwd);
//        Statement statement = connection.createStatement();
//        statement.execute(
//                SqlConstants.IU_OL_LENGTH_OF_DAY +
//                SqlConstants.IU_OL_DAYS_OF_MONTH +
//                SqlConstants.IU_OL_DAYS_OF_WEEK);
//        statement.execute(SqlConstants.GET_DEV_OL_LEN);
//        statement.close();

//        String devid = "013361084100";
//        Long start = 1555049425929L;
        Long start = 0L;
        Long end = 1555407251000L; // 时间戳
        CallableStatement iuProc = connection.prepareCall("{? = call f_get_dev_ol_len(?, ?, ?)}");

        String[] devlist = new String[] {
                "001461005100",
                "001461005700",
                "120177166300",
                "043700200000",
                "014602954300",
                "120186895600",
                "120175949400",
                "120168859400",
                "001461006000",
                "017753101400",
                "001605100800",
                "120175954400",
                "001602136400",
                "120177118400",
                "009511621700",
                "017763363200",
                "009511621600",
                "001802500100",
                "017305413600",
                "009511622000",
                "001705200600",
                "001461006700",
                "009511621500",
                "001461007800",
                "009511628300",
                "120173479400",
                "120168854500",
                "001461007100",
                "120186888100",
                "120186915200",
                "009511621300",
                "018100398400",
                "120174679800",
                "120173487700",
                "001453854000",
                "120175976700",
                "013361084100",
                "120173480200",
                "120173483600",
                "120168855200",
                "120186918600",
                "120186898000",
                "120168872700",
                "120186899800",
                "120173482800",
                "120175974200",
                "013361082400",
                "120186925100",
                "001802500200",
                "120174660800",
                "001800400100",
                "120174678000",
                "064684759700",
                "001453854500",
                "120186905300",
                "120175955100",
                "120175950200",
                "120175973400",
                "120173484400",
                "009511624800",
                "120175948600",
                "001705200700",
                "120168856000",
                "013253174300",
                "001703613400",
                "120168858600",
                "009511625000",
                "001605303200",
                "120186920200",
                "013361084200",
                "120186916000",
                "120186923600",
                "120174676400",
                "015376171900",
                "120174668100",
                "001453853400",
                "120168873500"
        };
        for(String deviceid: devlist){
            iuProc.registerOutParameter(1, Types.VARCHAR);
            iuProc.setString(2, deviceid);
            iuProc.setTimestamp(3, new Timestamp(start));
            iuProc.setTimestamp(4, new Timestamp(end));
            iuProc.execute();
            String ol_length = iuProc.getString(1);
            logger.info("Get_dev_ol_len({},{},{})={}", deviceid, start, end, ol_length);
        }
        iuProc.close();

        connection.close();
    }
}
