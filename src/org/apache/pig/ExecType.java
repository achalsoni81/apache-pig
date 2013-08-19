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

package org.apache.pig;

import java.io.Serializable;
import java.util.Properties;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.LocalExecType;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRExecType;
import org.apache.pig.impl.PigContext;

/**
 * The type of query execution
 */
public abstract class ExecType implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Given a string, determine the exec type.
     * @param execString accepted values are 'local', 'mapreduce', and 'mapred'
     * @return exectype as ExecType
     */
    public static ExecType fromString(String execString) throws ExecException {
        if (execString.equalsIgnoreCase("local")) {
            return LOCAL;
        } else if (execString.equalsIgnoreCase("mapred") ||
                   execString.equalsIgnoreCase("mapreduce")) {
            return MAPREDUCE;
        } else {
            int errCode = 2040;
            String msg = "Using custom ExecutionEngine. Use new ExecType interface instead.";
            throw new ExecException(msg, errCode);
        }
    }

    public static final LocalExecType LOCAL = new LocalExecType();
    public static final MRExecType MAPREDUCE = new MRExecType();

    /**
     * An ExecType is selected based off the Properties for the given 
     * script. There may be multiple settings that trigger the selection
     * of a given ExecType. For example, distributed MR mode is currently
     * triggered if the user specifies "mapred" or "mapreduce". It is 
     * desirable to override the toString method of the given ExecType
     * to uniquely identify the ExecType. 
     * 
     * The initialize method should return a reference to itself if the 
     * ExecType is chosen, and null if it is not. This is in compliance 
     * with the Java ServiceLoader framework that will be used to iterate 
     * through and select the correct ExecType. 
     */
    public abstract boolean accepts(Properties properties);

    /**
     * Returns the Execution Engine that this ExecType is associated with.
     * Once the ExecType the script is running in is determined by the 
     * PigServer, it will then call this method to get an instance of the
     * ExecutionEngine associated with this ExecType to delegate all further 
     * execution to on the backend.
     */
    public abstract ExecutionEngine getExecutionEngine(PigContext pigContext);

    /**
     * Returns the Execution Engine class that this ExecType is associated 
     * with. This method simply returns the class of the ExecutionEngine 
     * associated with this ExecType, and not an instance of it.
     */
    public abstract Class<? extends ExecutionEngine> getExecutionEngineClass();

   /**
    * An ExecType is classified as local if it runs in-process and through
    * the local filesystem. While an ExecutionEngine may have a more nuanced
    * notion of local mode, these are the qualifications Pig requires.
    * ExecutionEngines can extend the ExecType interface to additionally 
    * differentiate between ExecTypes as necessary. 
    */
    public abstract boolean isLocal();
}
