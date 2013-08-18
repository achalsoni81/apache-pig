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
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.LocalExecType;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRExecType;

/**
 * The type of query execution
 */
public enum ExecType implements Serializable {
    /**
     * Run everything on the local machine
     */
    LOCAL,
    /**
     * Use the Hadoop Map/Reduce framework
     */
    MAPREDUCE,
    /**
     * Use a custom framework
     */
   CUSTOM
   ;
    
    public static final LocalExecType localExecType = new LocalExecType();
    public static final MRExecType MRExecType = new MRExecType();

    /**
     * Given a string, determine the exec type.
     * @param execString accepted values are 'local', 'mapreduce', and 'mapred'
     * @return exectype as ExecType
     */
    public static ExecType fromString(String execString) throws PigException {
        if (execString.equals("mapred")) {
            return MAPREDUCE;
        } else {
            try {
                return ExecType.valueOf(execString.toUpperCase());
            } catch (IllegalArgumentException e) {
                int errCode = 2040;
                String msg = "Unknown exec type: " + execString;
                throw new PigException(msg, errCode, e);
            }
        }
    }
    
    
    public static org.apache.pig.backend.executionengine.ExecType getNewExecType(ExecType execType) throws PigException {
    
      switch (execType) {
         case LOCAL:
          return localExecType;
         case MAPREDUCE:
         {
           return MRExecType;
         }
         default:
         {
          int errCode = 2040;
             String msg = "Using custom ExecutionEngine. Use new ExecType interface instead.";
             throw new PigException(msg, errCode);
     }
  
    }
}
}
