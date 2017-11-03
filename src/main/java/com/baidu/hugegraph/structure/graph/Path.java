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

package com.baidu.hugegraph.structure.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.baidu.hugegraph.serializer.PathDeserializer;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = PathDeserializer.class)
public class Path {

    @JsonProperty
    private List<Object> labels;
    @JsonProperty
    private List<Object> objects;

    public Path() {
        this.labels = new ArrayList<>();
        this.objects = new ArrayList<>();
    }

    public List<Object> labels() {
        return Collections.unmodifiableList(this.labels);
    }

    public void labels(Object... labels) {
        this.labels.addAll(Arrays.asList(labels));
    }

    public List<Object> objects() {
        return Collections.unmodifiableList(this.objects);
    }

    public void objects(Object... objects) {
        this.objects.addAll(Arrays.asList(objects));
    }

    @Override
    public String toString() {
        return String.format("{labels=%s, objects=%s}",
                             this.labels, this.objects);
    }
}
