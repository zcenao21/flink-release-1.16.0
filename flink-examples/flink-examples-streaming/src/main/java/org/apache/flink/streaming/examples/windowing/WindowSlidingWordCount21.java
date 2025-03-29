/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.examples.windowing.will.WaterSensor;
import org.apache.flink.streaming.examples.windowing.will.WaterSensorMapFunction;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Implements a windowed version of the streaming "WordCount" program.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>
 * WordCount --input &lt;path&gt; --output &lt;path&gt; --window &lt;n&gt; --slide &lt;n&gt;</code>
 * <br>
 * If no parameters are provided, the program is run with default data from {@link WordCountData}.
 *
 * <p>This example shows how to:
 *
 * <ul>
 *   <li>write a simple Flink Streaming program,
 *   <li>use tuple data types,
 *   <li>use basic windowing abstractions.
 * </ul>
 */
public class WindowSlidingWordCount21 {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> waterSensorDs =
                env
                        .socketTextStream("127.0.0.1", 2121)
                        .map(new WaterSensorMapFunction())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                    .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                        );

        SingleOutputStreamOperator<WaterSensor> outST = waterSensorDs
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<WaterSensor, WaterSensor, String, TimeWindow>() {
                    @Override
                    public void process(
                            String s,
                            ProcessWindowFunction<WaterSensor, WaterSensor, String, TimeWindow>.Context context,
                            Iterable<WaterSensor> elements,
                            Collector<WaterSensor> out) throws Exception {
                        WaterSensor his = new WaterSensor(null, 0, 0);
                        for (WaterSensor element : elements) {
                            if (his.getId() == null){
                                his.setId(element.getId());
                                his.setTs(context.window().getEnd());
                            }
                            his.setVc(element.getVc() + his.getVc());
                        }
                        out.collect(his);
                    }
                });

                outST.print("21");


        env.execute("WindowWordCount");
    }
}
