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

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.client.RestClient;
import com.baidu.hugegraph.exception.InvalidResponseException;
import com.baidu.hugegraph.rest.RestResult;
import com.baidu.hugegraph.structure.constant.GraphMode;
import com.baidu.hugegraph.structure.constant.GraphReadMode;
import com.baidu.hugegraph.structure.constant.HugeType;
import com.google.common.collect.ImmutableMap;

public class GraphsAPI extends API {

    private static final String CONFIRM_MESSAGE = "confirm_message";
    private static final String DELIMITER = "/";
    private static final String MODE = "mode";
    private static final String GRAPH_READ_MODE = "graph_read_mode";

    public GraphsAPI(RestClient client) {
        super(client);
        this.path(this.type());
    }

    @Override
    protected String type() {
        return HugeType.GRAPHS.string();
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> create(String name, String cloneGraphName,
                                      String configText) {
        this.client.checkApiVersion("0.67", "dynamic graph add");
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
        Map<String, Object> params = null;
        if (cloneGraphName != null && !cloneGraphName.isEmpty()) {
            params = ImmutableMap.of("clone_graph_name", cloneGraphName);
        }
        RestResult result = this.client.post(joinPath(this.path(), name),
                                             configText, headers, params);
        return result.readObject(Map.class);
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

    public void clear(String graph, String message) {
        this.client.delete(joinPath(this.path(), graph, "clear"),
                           ImmutableMap.of(CONFIRM_MESSAGE, message));
    }

    public void delete(String graph, String message) {
        this.client.checkApiVersion("0.67", "dynamic graph delete");
        this.client.delete(joinPath(this.path(), graph),
                           ImmutableMap.of(CONFIRM_MESSAGE, message));
    }

    public void mode(String graph, GraphMode mode) {
        // NOTE: Must provide id for PUT. If use "graph/mode", "/" will
        // be encoded to "%2F". So use "mode" here although inaccurate.
        this.client.put(joinPath(this.path(), graph), MODE, mode);
    }

    public GraphMode mode(String graph) {
        RestResult result =  this.client.get(joinPath(this.path(), graph),
                                             MODE);
        @SuppressWarnings("unchecked")
        Map<String, String> mode = result.readObject(Map.class);
        String value = mode.get(MODE);
        if (value == null) {
            throw new InvalidResponseException(
                      "Invalid response, expect 'mode' in response");
        }
        try {
            return GraphMode.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new InvalidResponseException(
                      "Invalid GraphMode value '%s'", value);
        }
    }

    public void readMode(String graph, GraphReadMode readMode) {
        this.client.checkApiVersion("0.59", "graph read mode");
        // NOTE: Must provide id for PUT. If use "graph/graph_read_mode", "/"
        // will be encoded to "%2F". So use "graph_read_mode" here although
        // inaccurate.
        this.client.put(joinPath(this.path(), graph),
                        GRAPH_READ_MODE, readMode);
    }

    public GraphReadMode readMode(String graph) {
        this.client.checkApiVersion("0.59", "graph read mode");
        RestResult result =  this.client.get(joinPath(this.path(), graph),
                                             GRAPH_READ_MODE);
        @SuppressWarnings("unchecked")
        Map<String, String> readMode = result.readObject(Map.class);
        String value = readMode.get(GRAPH_READ_MODE);
        if (value == null) {
            throw new InvalidResponseException(
                      "Invalid response, expect 'graph_read_mode' in response");
        }
        try {
            return GraphReadMode.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new InvalidResponseException(
                      "Invalid GraphReadMode value '%s'", value);
        }
    }

    private static String joinPath(String path, String id) {
        return String.join(DELIMITER, path, id);
    }

    private static String joinPath(String path, String id, String action) {
        return String.join(DELIMITER, path, id, action);
    }
}
