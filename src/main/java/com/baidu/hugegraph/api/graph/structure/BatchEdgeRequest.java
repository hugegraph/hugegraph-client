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

package com.baidu.hugegraph.api.graph.structure;

import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.util.E;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BatchEdgeRequest {

    // TODO: Edge seems OK, should we keep the JsonEdge's code?
    @JsonProperty("edges")
    private List<Edge> jsonEdges;
    @JsonProperty("update_strategies")
    private Map<String, UpdateStrategy> updateStrategies;
    @JsonProperty("check_vertex")
    private boolean checkVertex;
    @JsonProperty("create_if_not_exist")
    private boolean createIfNotExist;

    public BatchEdgeRequest() {
        this.jsonEdges = null;
        this.updateStrategies = null;
        this.checkVertex = false;
        this.createIfNotExist = true;
    }

    @Override
    public String toString() {
        return String.format("BatchEdgeRequest{jsonEdges=%s," +
                             "updateStrategies=%s," +
                             "checkVertex=%s,createIfNotExist=%s}",
                             this.jsonEdges, this.updateStrategies,
                             this.checkVertex, this.createIfNotExist);
    }

    public static class Builder {

        private BatchEdgeRequest req;

        public Builder() {
            this.req = new BatchEdgeRequest();
        }

        public Builder edges(List<Edge> edges) {
            this.req.jsonEdges = edges;
            return this;
        }

        public Builder updateStrategies(Map<String, UpdateStrategy> maps) {
            this.req.updateStrategies = maps;
            return this;
        }

        public Builder checkVertex(boolean checkVertex) {
            this.req.checkVertex = checkVertex;
            return this;
        }

        public Builder createIfNotExist(boolean createIfNotExist) {
            this.req.createIfNotExist = createIfNotExist;
            return this;
        }

        public BatchEdgeRequest build() {
            E.checkArgumentNotNull(req, "BatchEdgeRequest cannot be null");
            E.checkArgumentNotNull(req.jsonEdges,
                                   "Parameter 'edges' cannot be null");
            E.checkArgument(req.updateStrategies != null &&
                            !req.updateStrategies.isEmpty(),
                            "Parameter 'update_strategies' cannot be empty");
            // Not support createIfNotExist equals false now
            E.checkArgument(req.createIfNotExist == true, "Parameter " +
                            "'create_if_not_exist' is not supported now");

            return this.req;
        }
    }

    @JsonIgnoreProperties(value = {"type"})
    private class JsonEdge {
        @JsonProperty("id")
        private Object id;
        @JsonProperty("label")
        private String label;
        @JsonProperty("properties")
        private Map<String, Object> properties;
        @JsonProperty("outV")
        private Object source;
        @JsonProperty("outVLabel")
        private String sourceLabel;
        @JsonProperty("inV")
        private Object target;
        @JsonProperty("inVLabel")
        private String targetLabel;
        @JsonProperty("type")
        private String type;

    }
}
