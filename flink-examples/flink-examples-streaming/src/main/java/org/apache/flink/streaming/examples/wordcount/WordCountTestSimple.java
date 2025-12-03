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

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.examples.wordcount.util.CLI;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram over text
 * files. This Job can be executed in both streaming and batch execution modes.
 *
 * <p>The input is a [list of] plain text file[s] with lines separated by a newline character.
 *
 * <p>Usage:
 *
 * <ul>
 *   <li><code>--input &lt;path&gt;</code>A list of input files and / or directories to read. If no
 *       input is provided, the program is run with default data from {@link WordCountData}.
 *   <li><code>--discovery-interval &lt;duration&gt;</code>Turns the file reader into a continuous
 *       source that will monitor the provided input directories every interval and read any new
 *       files.
 *   <li><code>--output &lt;path&gt;</code>The output directory where the Job will write the
 *       results. If no output path is provided, the Job will print the results to <code>stdout
 *       </code>.
 *   <li><code>--execution-mode &lt;mode&gt;</code>The execution mode (BATCH, STREAMING, or
 *       AUTOMATIC) of this pipeline.
 * </ul>
 *
 * <p>This example shows how to:
 *
 * <ul>
 *   <li>Write a simple Flink DataStream program
 *   <li>Use tuple data types
 *   <li>Write and use a user-defined function
 * </ul>
 */
public class WordCountTestSimple {

    public static void main(String[] args) throws Exception {
        final CLI params = CLI.fromArgs(args);

        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8081);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration);
        env.setParallelism(10);

        env.setRuntimeMode(params.getExecutionMode());

        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text;
        env.getConfig().setAutoWatermarkInterval(500);
        DataStream<String> source = env
                .socketTextStream("localhost",2121);
        text = source;

        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap(new Tokenizer())
                        .name("tokenizer")
                        .keyBy(value -> value.f0)
                        .sum(1)
                        .name("counter");

        counts.print().name("print-sink");
        env.execute("WordCount");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

    /**
     * TTL设置，不过没效果，不对.
     */
    public static final class TTLTokenizer extends RichFlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1808329479322205953L;
        private transient ValueState<Tuple2<String, Integer>> sumRes;

        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(
                        org.apache.flink.api.common.time.Time.milliseconds(3))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupFullSnapshot().build();

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            Tuple2<String, Integer> currentSum = sumRes.value();
            currentSum.f1 += 1;
            sumRes.update(currentSum);
            out.collect(new Tuple2<>(currentSum.f0,currentSum.f1));
            sumRes.clear();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple2<String, Integer>> descriptor = new ValueStateDescriptor<Tuple2<String, Integer>>(
                    "sumRes",
                    TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                        @Override
                        public TypeInformation<Tuple2<String, Integer>> getTypeInfo() {
                            return super.getTypeInfo();
                        }
                    }),
                    null
            );
            descriptor.enableTimeToLive(ttlConfig);
            sumRes = getRuntimeContext().getState(descriptor);
        }
    }
}
