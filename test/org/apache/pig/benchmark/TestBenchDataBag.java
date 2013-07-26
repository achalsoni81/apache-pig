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

package org.apache.pig.benchmark;

import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;

public class TestBenchDataBag {

    private static int MAX_TUPLES = 100000;
    private static int BAGS_PER_TEST = 100;
    private static Tuple [] data;

    private static final BagFactory BF = BagFactory.getInstance();
    private static final TupleFactory TF = TupleFactory.getInstance();

    // purely there for side effects so bag creation doesn't get JITed out
    private long l = 0;

    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    @BenchmarkOptions(benchmarkRounds = 100, warmupRounds = 1, callgc = false,
            clock = com.carrotsearch.junitbenchmarks.Clock.NANO_TIME)

    /** Prepare tuples of between 0 and 10 ints for tests */
    @BeforeClass
    public static void prepare() {
        data = new Tuple [MAX_TUPLES];
        final Random random = new Random(132);
        for (int i = 0; i < MAX_TUPLES; i++) {
            int num = random.nextInt(10);
            Tuple tup = TF.newTuple(num);
            for (int j = 0; j < num; j++) {
                try {
                    tup.set(j, 100 + num);
                } catch (ExecException e) {
                    //omfg can we kill this exception please.
                }
            }
            data[i] = tup;
        }
    }

    @After
    public  void finish() {
        //System.out.println("Finished Data Bag Benchmark. l = " + l);
    }

    @Test
    public void oneElemBag() throws Exception {
        fillBags(1);
    }

    @Test
    public void twentyElemBag() throws Exception {
        fillBags(20);
    }

    @Test
    public void hundredElemBag() throws Exception {
        fillBags(100);
    }

    @Test
    public void thousandElemBag() throws Exception {
        fillBags(1000);
    }

    // Test the speed of writing to a bag in memory: Mark's test.
    @Test
    public void testDefaultSpeed() throws Exception {
        int iterations = 100;
        DataBag b = BagFactory.getInstance().newDefaultBag();

        Tuple tuple = TupleFactory.getInstance().newTuple(4);
        tuple.append(false);
        tuple.append(true);
        tuple.append(1);
        tuple.append(0.0f);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            b.add(tuple);
        }
        long endTime = System.nanoTime();
        System.err.println((endTime - startTime) / iterations + " ns");
    }

    public void fillBags(int tuplesPerBag) throws Exception {
        for (int iter = 0; iter < BAGS_PER_TEST; iter++) {
            DataBag bag = BF.newDefaultBag();
            for (int i = 0; i < tuplesPerBag; i++) {
                bag.add(data[i]);
            }
            l += bag.size();
        }
    }
}

