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

package com.baidu.hugegraph.driver;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.baidu.hugegraph.api.graphs.GraphsAPI;
import com.baidu.hugegraph.client.RestClient;
import com.baidu.hugegraph.rest.ClientException;
import com.baidu.hugegraph.structure.constant.GraphMode;
import com.baidu.hugegraph.structure.constant.GraphReadMode;

public class GraphsManager {

    private GraphsAPI graphsAPI;

    public GraphsManager(RestClient client) {
        this.graphsAPI = new GraphsAPI(client);
    }

    public Map<String, String> createGraph(String name, String config) {
        return this.createGraph(name, null, config);
    }

    public Map<String, String> createGraph(String name, String cloneGraphName,
                                           String config) {
        return this.graphsAPI.create(name, cloneGraphName, config);
    }

    public Map<String, String> createGraph(String name, File file) {
        String config;
        try {
            config = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new ClientException("Failed to read config file: %s", file);
        }
        return this.createGraph(name, config);
    }

    public Map<String, String> createGraph(String name, String cloneGraphName,
                                           File file) {
        String config;
        try {
            config = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new ClientException("Failed to read config file: %s", file);
        }
        return this.createGraph(name, cloneGraphName, config);
    }

    public Map<String, String> getGraph(String graph) {
        return this.graphsAPI.get(graph);
    }

    public List<String> listGraph() {
        return this.graphsAPI.list();
    }

    public void clear(String graph, String message) {
        this.graphsAPI.clear(graph, message);
    }

    public void remove(String graph, String message) {
        this.graphsAPI.delete(graph, message);
    }

    public void mode(String graph, GraphMode mode) {
        this.graphsAPI.mode(graph, mode);
    }

    public GraphMode mode(String graph) {
        return this.graphsAPI.mode(graph);
    }

    public void readMode(String graph, GraphReadMode readMode) {
        this.graphsAPI.readMode(graph, readMode);
    }

    public GraphReadMode readMode(String graph) {
        return this.graphsAPI.readMode(graph);
    }
}
