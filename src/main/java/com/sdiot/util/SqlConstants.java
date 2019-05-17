package com.sdiot.util;

public final class SqlConstants {
    public static final String DB_URL = "jdbc:postgresql://192.168.10.22:5432/iot";
    public static final String DB_USER = "postgres";
    public static final String DB_PWD = "postgres";

    public static final String IU_OL_DAYS_OF_MONTH =
            "CREATE OR REPLACE FUNCTION f_iu_ol_days_of_month(v_devid TEXT, v_month TEXT, v_ol_days INT, v_lut TIMESTAMP)\n" +
            "   RETURNS VOID AS\n" +
            "$$\n" +
            "BEGIN\n" +
            "    LOOP\n" +
            "        -- first try to update the key\n" +
            "        UPDATE stat_dev_ol_days_of_month\n" +
            "           SET online_days = v_ol_days, last_update_time = v_lut\n" +
            "           WHERE devid = v_devid AND month = v_month;\n" +
            "        IF found THEN\n" +
            "            RETURN;\n" +
            "        END IF;\n" +
            "        -- not there, so try to insert the key\n" +
            "        -- if someone else inserts the same key concurrently,\n" +
            "        -- we could get a unique-key failure\n" +
            "        BEGIN\n" +
            "            INSERT INTO stat_dev_ol_days_of_month(devid,month,online_days,last_update_time)\n" +
            "            VALUES (v_devid, v_month, v_ol_days,v_lut);\n" +
            "            RETURN;\n" +
            "        EXCEPTION WHEN unique_violation THEN\n" +
            "            -- Do nothing, and loop to try the UPDATE again.\n" +
            "        END;\n" +
            "    END LOOP;\n" +
            "END;\n" +
            "$$\n" +
            "LANGUAGE plpgsql;"
            ;

    public static final String IU_OL_DAYS_OF_WEEK =
            "CREATE OR REPLACE FUNCTION f_iu_ol_days_of_week(v_devid TEXT, v_week TEXT, v_ol_days INT, v_lut TIMESTAMP)\n" +
            "   RETURNS VOID AS\n" +
            "$$\n" +
            "BEGIN\n" +
            "    LOOP\n" +
            "        -- first try to update the key\n" +
            "        UPDATE stat_dev_ol_days_of_week\n" +
            "           SET online_days = v_ol_days, last_update_time = v_lut\n" +
            "           WHERE devid = v_devid AND week = v_week;\n" +
            "        IF found THEN\n" +
            "            RETURN;\n" +
            "        END IF;\n" +
            "        -- not there, so try to insert the key\n" +
            "        -- if someone else inserts the same key concurrently,\n" +
            "        -- we could get a unique-key failure\n" +
            "        BEGIN\n" +
            "            INSERT INTO stat_dev_ol_days_of_week(devid,week,online_days,last_update_time)\n" +
            "               VALUES (v_devid, v_week, v_ol_days,v_lut);\n" +
            "            RETURN;\n" +
            "        EXCEPTION WHEN unique_violation THEN\n" +
            "            -- Do nothing, and loop to try the UPDATE again.\n" +
            "        END;\n" +
            "    END LOOP;\n" +
            "END;\n" +
            "$$\n" +
            "LANGUAGE plpgsql;"
            ;

    public static final String IU_OL_LENGTH_OF_DAY =
            "CREATE OR REPLACE FUNCTION f_iu_ol_length_of_day(v_devid TEXT, v_ol_date DATE , v_ol_length BIGINT, v_lut TIMESTAMP)\n" +
            "   RETURNS VOID AS\n" +
            "$$\n" +
            "BEGIN\n" +
            "    LOOP\n" +
            "        -- first try to update the key\n" +
            "        UPDATE stat_dev_ol_length_of_day\n" +
            "           SET ol_length = v_ol_length, last_update_time = v_lut\n" +
            "           WHERE devid = v_devid AND ol_date = v_ol_date;\n" +
            "        IF found THEN\n" +
            "            RETURN;\n" +
            "        END IF;\n" +
            "        -- not there, so try to insert the key\n" +
            "        -- if someone else inserts the same key concurrently,\n" +
            "        -- we could get a unique-key failure\n" +
            "        BEGIN\n" +
            "            INSERT INTO stat_dev_ol_length_of_day(devid,ol_date,ol_length,last_update_time)\n" +
            "               VALUES (v_devid, v_ol_date,v_ol_length,v_lut);\n" +
            "            RETURN;\n" +
            "        EXCEPTION WHEN unique_violation THEN\n" +
            "            -- Do nothing, and loop to try the UPDATE again.\n" +
            "        END;\n" +
            "    END LOOP;\n" +
            "END;\n" +
            "$$\n" +
            "LANGUAGE plpgsql;"
            ;

    public static final String IU_OL_LENGTH_OF_HALF_HOUR =
            "CREATE OR REPLACE FUNCTION f_iu_ol_length_of_half_hour(v_devid TEXT, v_ol_time TIMESTAMP , v_ol_length BIGINT, v_lut TIMESTAMP)\n" +
            "   RETURNS VOID AS\n" +
            "$$\n" +
            "BEGIN\n" +
            "    LOOP\n" +
            "        -- first try to update the key\n" +
            "        UPDATE stat_dev_ol_length_of_halfhour\n" +
            "           SET ol_length = v_ol_length, last_update_time = v_lut\n" +
            "           WHERE devid = v_devid AND ol_time = v_ol_time;\n" +
            "        IF found THEN\n" +
            "            RETURN;\n" +
            "        END IF;\n" +
            "        -- not there, so try to insert the key\n" +
            "        -- if someone else inserts the same key concurrently,\n" +
            "        -- we could get a unique-key failure\n" +
            "        BEGIN\n" +
            "            INSERT INTO stat_dev_ol_length_of_halfhour(devid,ol_time,ol_length,last_update_time)\n" +
            "               VALUES (v_devid, v_ol_time,v_ol_length,v_lut);\n" +
            "            RETURN;\n" +
            "        EXCEPTION WHEN unique_violation THEN\n" +
            "            -- Do nothing, and loop to try the UPDATE again.\n" +
            "        END;\n" +
            "    END LOOP;\n" +
            "END;\n" +
            "$$\n" +
            "LANGUAGE plpgsql;"
            ;

    public static final String IU_OL_LENGTH_SEGMENT =
            "CREATE OR REPLACE FUNCTION f_iu_ol_length_segment(v_devid TEXT, v_ol_start TIMESTAMP , v_ol_end TIMESTAMP, v_lut TIMESTAMP)\n" +
            "   RETURNS VOID AS\n" +
            "$$\n" +
            "BEGIN\n" +
            "    LOOP\n" +
            "        -- first try to update the key\n" +
            "        UPDATE stat_dev_ol_length_segment\n" +
            "           SET ol_end = v_ol_end, last_update_time = v_lut\n" +
            "           WHERE devid = v_devid AND ol_start = v_ol_start;\n" +
            "        IF found THEN\n" +
            "            RETURN;\n" +
            "        END IF;\n" +
            "        -- not there, so try to insert the key\n" +
            "        -- if someone else inserts the same key concurrently,\n" +
            "        -- we could get a unique-key failure\n" +
            "        BEGIN\n" +
            "            INSERT INTO stat_dev_ol_length_segment(devid,ol_start,ol_end,last_update_time)\n" +
            "               VALUES (v_devid, v_ol_start,v_ol_end,v_lut);\n" +
            "            RETURN;\n" +
            "        EXCEPTION WHEN unique_violation THEN\n" +
            "            -- Do nothing, and loop to try the UPDATE again.\n" +
            "        END;\n" +
            "    END LOOP;\n" +
            "END;\n" +
            "$$\n" +
            "LANGUAGE plpgsql;"
            ;

    public static final String GET_DEV_OL_LEN =
            "CREATE OR REPLACE FUNCTION f_get_dev_ol_len(v_devid TEXT, v_start_time TIMESTAMP , v_end_time TIMESTAMP)\n" +
            "   RETURNS TEXT AS\n" +
            "$$\n" +
            "DECLARE\n" +
            "   online_len INTERVAL;\n" +
            "BEGIN\n" +
            "   SELECT INTO online_len SUM\n" +
            "       (\n" +
            "        CASE\n" +
            "            WHEN v_start_time BETWEEN ol_start AND ol_end	THEN\n" +
            "                age(ol_end, v_start_time)\n" +
            "            WHEN v_end_time BETWEEN ol_start AND ol_end THEN\n" +
            "                age(v_end_time, ol_start)\n" +
            "            ELSE\n" +
            "                age(ol_end, ol_start)\n" +
            "            END\n" +
            "       )\n" +
            "   FROM stat_dev_ol_length_segment\n" +
            "   WHERE devid = v_devid AND (ol_start, ol_end) OVERLAPS (v_start_time, v_end_time);\n" +
            "   RETURN to_char(online_len, 'YYYY-MM-DD HH24:MI:SS.MS');\n" +
            "END;\n" +
            "$$\n" +
            "LANGUAGE plpgsql;"
            ;
}
