/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.structure.traverser;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RepeatEdgeStep extends EdgeStep {

    @JsonProperty("max_times")
    public int maxTimes = 1;

    private RepeatEdgeStep() {
        super();
        this.maxTimes = 1;
    }

    @Override
    public String toString() {
        return String.format("RepeatEdgeStep{direction=%s,labels=%s," +
                             "properties=%s,degree=%s,skipDegree=%s," +
                             "maxTimes=%s}",
                             this.direction, this.labels, this.properties,
                             this.degree, this.skipDegree, this.maxTimes);
    }
}
