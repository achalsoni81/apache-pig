/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.pig.test.udf.storefunc;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigCounters;
import org.apache.pig.StoreFuncWrapper;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.tools.counters.PigCounterHelper;

/**
 * This is a simple StoreFunc that wraps PigStorage. It counts the number of
 * files it creates before writing output.
 */
public class PigStorageWithFileCount extends StoreFuncWrapper {
    boolean countFiles = false;
    private static PigCounterHelper counter = new PigCounterHelper();

    public PigStorageWithFileCount() {
        this.setStoreFunc(new PigStorage());
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        countFiles = job.getConfiguration().get(PigConfiguration.PROP_JOB_TERMINATION_COUNT_LIMIT, null) != null;
        super.setStoreLocation(location, job);
    }

    @Override
    public void putNext(Tuple tuple) throws IOException {
       if (countFiles) {
            counter.incrCounter(PigCounters.JOB_TERMINATION_COUNT, 1L);
            countFiles = false;
        }
        super.putNext(tuple);
    }
}
