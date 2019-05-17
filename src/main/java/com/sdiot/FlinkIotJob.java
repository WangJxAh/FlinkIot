/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sdiot;

import com.sdiot.connector.hbase.HBaseOLSegmentWriter;
import com.sdiot.connector.hbase.HBaseReader;
import com.sdiot.connector.pgsql.PgsqlOLDaysOfMonthWriter;
import com.sdiot.connector.pgsql.PgsqlOLDaysOfWeekWriter;
import com.sdiot.connector.pgsql.PgsqlOLLenOfDayWriter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class FlinkIotJob {
    private static final Logger logger = LoggerFactory.getLogger(FlinkIotJob.class);

    // constants
    private static final int WINDOW_LEN_BY_HOUR = 12;

    // util class
    private static class MS2DurMapFunction implements MapFunction<Tuple4<String, String, Long, Integer>,
            Tuple4<String, String, String, Integer>> {

        @Override
        public Tuple4<String, String, String, Integer> map(Tuple4<String, String, Long, Integer> element) throws Exception {
            return new Tuple4<>(element.f0,
                    element.f1,
                    Duration.ofMillis(element.f2).toString(),
                    element.f3
            );
        }
    }

    private static class Online {
        String devid;
        String dateOrWeek;
        long length = 0;
        boolean dayOnline = false;
        // temp value
        boolean onlinePre = false; // previous status
        long tsPre = 0; // previous timestamp
    }

    private static class AggrFunc implements AggregateFunction<Tuple4<String, String, Long, Integer>, Online,
            Tuple4<String, String, Long, Integer>> {

        @Override
        public Online createAccumulator() {
            return new Online();
        }

        @Override
        public Online add(Tuple4<String, String, Long, Integer> value, Online accumulator) {
            accumulator.devid = value.f0;
            accumulator.dateOrWeek = value.f1;

            // online length
            if (accumulator.onlinePre) {
                //
                accumulator.length += value.f2 - accumulator.tsPre;
            }

            // record temp value
            accumulator.tsPre = value.f2;
            accumulator.onlinePre = value.f3 == 1;
            // dayOnline needs only 1 assignment
            if (!accumulator.dayOnline)
                accumulator.dayOnline = accumulator.onlinePre;
            return accumulator;
        }

        @Override
        public Tuple4<String, String, Long, Integer> getResult(Online accumulator) {
            return new Tuple4<>(accumulator.devid,
                    accumulator.dateOrWeek,
                    accumulator.length,
                    accumulator.dayOnline ? 1 : 0);
        }

        @Override
        public Online merge(Online a, Online b) {
            a.length += b.length;
            a.dayOnline = a.dayOnline || b.dayOnline;
            return a;
        }
    }

    //
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.enableCheckpointing(5000);
        env.enableCheckpointing(12 * 60 * 60 * 1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         * 	env.readTextFile(textPath);
         *
         * then, transform the resulting DataStream<String> using operations
         * like
         * 	.filter()
         * 	.flatMap()
         * 	.join()
         * 	.coGroup()
         *
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * http://flink.apache.org/docs/latest/apis/streaming/index.html
         *
         */
        ///////////////////////////////////////////////////////////////////////////////
        // source
        DataStream<Tuple3<String, Long, Integer>> dataStream = env.addSource(new HBaseReader())
//				.setParallelism(1)
                ;

        ///////////////////////////////////////////////////////////////////////////////
        // event timestamp
        DataStream<Tuple3<String, Long, Integer>> dataStreamWithTimestamp =
                dataStream.assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Integer>>(Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(Tuple3<String, Long, Integer> element) {
                                return element.f1 + 8 * 60 * 60 * 1000;//tz
                            }
                        });
        ///////////////////////////////////////////////////////////////////////////////
        // process streams
//        processDataStream(dataStreamWithTimestamp);

        processOnlineDaysOfMonth(dataStreamWithTimestamp);
        processOnlineDaysOfWeek(dataStreamWithTimestamp);
        processOnlineLengthSegment(dataStreamWithTimestamp);
        ///////////////////////////////////////////////////////////////////////////////
        // execute program
        env.execute("Iot-FlinkStatJob");
    }

    private static void processDataStream(DataStream<Tuple3<String, Long, Integer>> dataStream) {
        processOnlineDaysOfMonth(dataStream);

        processOnlineDaysOfWeek(dataStream);

        processOnlineLengthOfDay(dataStream);

    }

    private static void processOnlineDaysOfMonth(DataStream<Tuple3<String, Long, Integer>> dataStream) {
        DataStream<Tuple3<String, String, Integer>> result =
                dataStream
                        .map(new MapFunction<Tuple3<String, Long, Integer>, Tuple3<String, String, Integer>>() {
                            @Override
                            public Tuple3<String, String, Integer> map(Tuple3<String, Long, Integer> value) throws Exception {
                                Date date = new Date(value.f1);

                                SimpleDateFormat monthFmt = new SimpleDateFormat("yyyy-MM-dd");

                                return new Tuple3<>(value.f0,
                                        monthFmt.format(date),
                                        value.f2);
                            }
                        }) // <devid, day, online>
                        .keyBy(0, 1)
                        .window(TumblingEventTimeWindows.of(Time.hours(24)))
                        .reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
                            @Override
                            public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> value1,
                                                                          Tuple3<String, String, Integer> value2) throws Exception {
                                if (value1.f2 == 1 || value2.f2 == 1)
                                    return new Tuple3<>(value1.f0, value1.f1, 1);
                                else
                                    return new Tuple3<>(value1.f0, value1.f1, 0);
                            }
                        })
                        .map(new MapFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>>() {
                            @Override
                            public Tuple3<String, String, Integer> map(Tuple3<String, String, Integer> value) throws Exception {
                                return new Tuple3<>(value.f0, value.f1.substring(0, 7), value.f2);
                            }
                        }) // <devid, month, online>
                        .keyBy(0, 1)
                        .sum(2);

        result
                .addSink(new PgsqlOLDaysOfMonthWriter()).name("PgsqlOLMonthWriter");
//                .setParallelism(1)
        ;
        result.print("每月在线天数");
    }

    private static void processOnlineDaysOfWeek(DataStream<Tuple3<String, Long, Integer>> dataStream) {
        DataStream<Tuple3<String, String, Integer>> result = dataStream
                .map(new MapFunction<Tuple3<String, Long, Integer>, Tuple4<String, String, String, Integer>>() {
                    @Override
                    public Tuple4<String, String, String, Integer> map(Tuple3<String, Long, Integer> value) throws Exception {
                        Date date = new Date(value.f1);

                        SimpleDateFormat monthFmt = new SimpleDateFormat("yyyy-MM-dd");
                        SimpleDateFormat weekFmt = new SimpleDateFormat("yyyy(ww)"); //W, week of month, w, week of year

                        return new Tuple4<>(value.f0,
                                monthFmt.format(date),
                                weekFmt.format(date),
                                value.f2);
                    }
                }) // <devid, day, week, online>
                .keyBy(0, 1)
                .window(TumblingEventTimeWindows.of(Time.hours(24)))
                .reduce(new ReduceFunction<Tuple4<String, String, String, Integer>>() {
                    @Override
                    public Tuple4<String, String, String, Integer> reduce(Tuple4<String, String, String, Integer> value1,
                                                                          Tuple4<String, String, String, Integer> value2) throws Exception {
                        return new Tuple4<>(value1.f0, value1.f1, value1.f2,
                                value1.f3 == 1 || value2.f3 == 1 ? 1 : 0);
                    }
                })
                .map(new MapFunction<Tuple4<String, String, String, Integer>, Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> map(Tuple4<String, String, String, Integer> value) throws Exception {
                        return new Tuple3<>(value.f0, value.f2, value.f3);
                    }
                }) // <devid, week, online>
                .keyBy(0, 1)
                .sum(2);
        result
                .addSink(new PgsqlOLDaysOfWeekWriter()).name("PqsqlOLWeekWriter");
//                .setParallelism(1)
        ;
        result.print("每周在线天数");
    }

    private static void processOnlineLengthOfDay(DataStream<Tuple3<String, Long, Integer>> dataStream) {
        DataStream<Tuple3<String, String, Long>> result = dataStream
                .map(new MapFunction<Tuple3<String, Long, Integer>, Tuple4<String, String, Long, Integer>>() {
                    @Override
                    public Tuple4<String, String, Long, Integer> map(Tuple3<String, Long, Integer> value) throws Exception {
                        Date date = new Date(value.f1);

                        SimpleDateFormat monthFmt = new SimpleDateFormat("yyyy-MM-dd");

                        return new Tuple4<>(value.f0,
                                monthFmt.format(date),
                                value.f1,
                                value.f2);
                    }
                }) // <devid, day, ts, online>
                .keyBy(0, 1)
                .window(TumblingEventTimeWindows.of(Time.hours(24)))
                .aggregate(new AggrFunc())
                .map(new MapFunction<Tuple4<String, String, Long, Integer>, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(Tuple4<String, String, Long, Integer> value) throws Exception {
                        return new Tuple3<>(value.f0, value.f1, value.f2);
                    }
                });
        result
                .addSink(new PgsqlOLLenOfDayWriter())
//                .setParallelism(1)
        ;
//        result.print("在线时长(每天)");
    }


    private static void processOnlineLengthSegment(DataStream<Tuple3<String, Long, Integer>> dataStream) {
        class Acc {
            String devid = "";
            long start = 0L;
            long end = 0L;
            int endStatus = 0; // status of the end ts: online or offline
        }
        DataStream<Tuple4<String, Long, Long, Integer>> result = dataStream
                .keyBy(0)
                .window(GlobalWindows.create())
                .trigger(PurgingTrigger.of(new Trigger<Tuple3<String, Long, Integer>, Window>() {
//                    private final ValueStateDescriptor<Tuple2<Long, Long>> stateDesc =
//                            new ValueStateDescriptor<>("last-element", new TupleSerializer<>(
//                                            (Class<Tuple2<Long, Long>>) (Class<?>) Tuple2.class,
//                                            new TypeSerializer<?>[] { LongSerializer.INSTANCE, LongSerializer.INSTANCE }
//                                    ));
                    private final ValueStateDescriptor<Long> stateDesc =
                        new ValueStateDescriptor<Long>("origin_ts", LongSerializer.INSTANCE);

                    @Override
                    public TriggerResult onElement(Tuple3<String, Long, Integer> element, long timestamp,
                                                   Window window, TriggerContext ctx) throws Exception {

                        ValueState<Long> lastElementState = ctx.getPartitionedState(stateDesc);
                        if (element.f2 == 0) {
                            lastElementState.clear();
                            return TriggerResult.FIRE;
                        }

                        if (lastElementState.value() == null)
                        {
                            lastElementState.update(element.f1);
                            return TriggerResult.CONTINUE;
                        }
                        if (element.f1 - lastElementState.value() >= WINDOW_LEN_BY_HOUR * 60 * 60 * 1000)
                        {
//                            lastElementState.update(element.f1);
                            lastElementState.clear();
                            return TriggerResult.FIRE;
                        }
                        return TriggerResult.CONTINUE;

                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(Window window, TriggerContext ctx) throws Exception {
                        ctx.getPartitionedState(stateDesc).clear();
                    }
                }))
                .aggregate(new AggregateFunction<Tuple3<String, Long, Integer>,
                        Acc, Tuple4<String, Long, Long, Integer>>() {

                    @Override
                    public Acc createAccumulator() {
                        return new Acc();
                    }

                    @Override
                    public Acc add(Tuple3<String, Long, Integer> value,
                                   Acc accumulator) {
                        accumulator.devid = value.f0;

                        if (accumulator.start == 0L)
                            accumulator.start = value.f1;
                        else
                            accumulator.start = Math.min(value.f1, accumulator.start);

                        accumulator.end = Math.max(value.f1, accumulator.end);
                        accumulator.endStatus = value.f2;

                        return accumulator;
                    }

                    @Override
                    public Tuple4<String, Long, Long, Integer> getResult(Acc accumulator) {
                              return new Tuple4<>(accumulator.devid, accumulator.start, accumulator.end, accumulator.endStatus);
                    }

                    @Override
                    public Acc merge(Acc a, Acc b) {
//                        a.start = Math.min(a.start, b.start);
//                        a.end = Math.max(a.end, b.end);
//
//                        return a;

                        return null;
                    }
                })
                .filter(new FilterFunction<Tuple4<String, Long, Long, Integer>>() {
                    @Override
                    public boolean filter(Tuple4<String, Long, Long, Integer> value) throws Exception {
                        return value.f1.compareTo(value.f2) < 0;
                    }
                });


        result.addSink(new HBaseOLSegmentWriter()).name("HBaseOLSegWriter");
//
        result.map(new MapFunction<Tuple4<String, Long, Long, Integer>, Tuple4<String, String, String, Integer>>() {
            @Override
            public Tuple4<String, String, String, Integer> map(Tuple4<String, Long, Long, Integer> value) throws Exception {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS");
                return new Tuple4<>(value.f0,
                        simpleDateFormat.format(new Date(value.f1)),
                        simpleDateFormat.format(new Date(value.f2)), value.f3);
            }
        }).print("在线时段");

    }
}
