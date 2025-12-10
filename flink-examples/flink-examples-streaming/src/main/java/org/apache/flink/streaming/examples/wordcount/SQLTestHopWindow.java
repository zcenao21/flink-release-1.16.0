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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.wordcount.util.CLI;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

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
public class SQLTestHopWindow {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {
        final CLI params = CLI.fromArgs(args);

        // Create the execution environment. This is the main entrypoint
        // to building a Flink application.
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 9091);
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        env.setRuntimeMode(params.getExecutionMode());
//
//        // This optional step makes the input parameters
//        // available in the Flink UI.
//        env.getConfig().setGlobalJobParameters(params);
//
//        DataStream<String> text;
//        env.getConfig().setAutoWatermarkInterval(500);
        // 设置自动产生输入，并设置输入格式
        DataStream<String> source = env
                .socketTextStream("localhost",9999);
//        text = source;
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStream<Tuple2<String, Integer>> input = source
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] ss = s.split(/**/"[,:\\s+()]");
                        for(String in: ss){
                            collector.collect(new Tuple2<>(in,1));
                        }
                    }
                });
        Table table = tEnv.fromDataStream(input,$("word"), $("cnt"), $("word_process_time").proctime());
        tEnv.createTemporaryView("wordTable", table);

        String query = " select"
                + " hop_start(word_process_time,interval '10' second,interval '5' second) as hop_start,"
                + " count(1)"
                + " from wordTable"
                + " group by hop(word_process_time,interval '10' second,interval '5' second)";
        TableResult tableRes = tEnv.executeSql(query);
//        DataStream<WCRes> result = tEnv.toDataStream(tableRes, WCRes.class);
//        result.print();
        tableRes.print();

        // Apache Flink applications are composed lazily. Calling execute
        // submits the Job and begins processing.
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
     * 定义聚合方式.
     */
    public static class WeightAgg extends AggregateFunction<Double, Tuple2<Integer, Integer>> {

        @Override
        public Double getValue(Tuple2<Integer, Integer> accVal) {
            return accVal.f0*1.0/accVal.f1;
        }

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return new Tuple2<>(0,0);
        }

        public void accumulate(Tuple2<Integer, Integer> accVal, int score, int weight) {
            accVal.f0 += score*weight;
            accVal.f1 += weight;
        }
    }

    /**
     * wordcount定义.
     */
    public static class WC {
        public String word;//hello
        public long frequency;//1
        public long timein;

        // public constructor to make it a Flink POJO
        public WC() {}

        public WC(String word, long frequency, long timein) {
            this.word = word;
            this.frequency = frequency;
            this.timein=timein;
        }

        @Override
        public String toString() {
            return "WC " + word + " " + frequency+" "+timein;
        }
    }

    /**
     * word count新定义.
     */
    public static class WCRes {
        public String word;//hello
        public long frequency;//1

        // public constructor to make it a Flink POJO
        public WCRes() {}

        public WCRes(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "WC Count: " + word + " " + frequency;
        }
    }
}
