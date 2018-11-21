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

package com.baidu.hugegraph.api.graphs;

import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.client.RestClient;
import com.baidu.hugegraph.exception.InvalidResponseException;
import com.baidu.hugegraph.rest.RestResult;
import com.baidu.hugegraph.structure.constant.GraphMode;
import com.baidu.hugegraph.structure.constant.HugeType;
import com.google.common.collect.ImmutableMap;

public class GraphsAPI extends API {

    public GraphsAPI(RestClient client) {
        super(client);
        this.path(this.type());
    }

    @Override
    protected String type() {
        return HugeType.GRAPHS.string();
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> get(String name) {
        RestResult result = this.client.get(this.path(), name);
        return result.readObject(Map.class);
    }

    public List<String> list() {
        RestResult result = this.client.get(this.path());
        return result.readList(this.type(), String.class);
    }

    public void mode(String graph, GraphMode mode) {
        String path = String.join("/", this.path(), graph);
        // NOTE: Must provide id for PUT. If use "graph/mode", "/" will
        // be encoded to "%2F". So use "mode" here although inaccurate.
        this.client.put(path, "mode", mode);
    }

    public GraphMode mode(String graph) {
        String path = String.join("/", this.path(), graph);
        RestResult result =  this.client.get(path, "mode");
        @SuppressWarnings("unchecked")
        Map<String, String> mode = result.readObject(Map.class);
        String value = mode.get("mode");
        if (value == null) {
            throw new InvalidResponseException("mode", "mode response");
        }
        try {
            return GraphMode.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new InvalidResponseException(
                      "Invalid GraphMode value '%s'", value);
        }
    }

    public void clear(String graph, String message) {
        String path = String.join("/", this.path(), graph, "clear");
        this.client.delete(path,
                           ImmutableMap.of("confirm_message", message));
    }
}
