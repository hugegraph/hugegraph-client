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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.util.E;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BatchVertexRequest {

    // TODO: Vertex seems OK, should we keep the JsonVertex's code?
    @JsonProperty("vertices")
    private List<Vertex> jsonVertices;
    @JsonProperty("update_strategies")
    private Map<String, UpdateStrategy> updateStrategies;
    @JsonProperty("create_if_not_exist")
    private boolean createIfNotExist;

    public BatchVertexRequest() {
        this.jsonVertices = null;
        this.updateStrategies = null;
        this.createIfNotExist = true;
    }

    @Override
    public String toString() {
        return String.format("BatchVertexRequest{jsonVertices=%s," +
                             "updateStrategies=%s,createIfNotExist=%s}",
                             this.jsonVertices, this.updateStrategies,
                             this.createIfNotExist);
    }

    public static class Builder {

        private BatchVertexRequest req;
        private List<JsonVertex.Builder> vertexBuilders;

        public Builder() {
            this.req = new BatchVertexRequest();
            //this.vertexBuilders = new ArrayList<>();
        }

        public JsonVertex.Builder vertices() {
            JsonVertex.Builder builder = new JsonVertex.Builder();
            this.vertexBuilders.add(builder);
            return builder;
        }

        public Builder vertices(List<Vertex> vertices) {
            this.req.jsonVertices = vertices;
            return this;
        }

        public Builder updateStrategies(Map<String, UpdateStrategy> maps) {
            this.req.updateStrategies = maps;
            return this;
        }

        public Builder createIfNotExist(boolean createIfNotExist) {
            this.req.createIfNotExist = createIfNotExist;
            return this;
        }

        public BatchVertexRequest build() {
            /*this.vertexBuilders.forEach(builder -> {
                req.jsonVertices.add(builder.build());
            });*/
            E.checkArgumentNotNull(req, "BatchVertexRequest cannot be null");
            E.checkArgumentNotNull(req.jsonVertices,
                                   "Parameter 'vertices' cannot be null");
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
    private static class JsonVertex {
        @JsonProperty("id")
        private Object id;
        @JsonProperty("label")
        private String label;
        @JsonProperty("properties")
        private Map<String, Object> properties;
        @JsonProperty("type")
        private String type;

        public JsonVertex() {
            this.id = null;
            this.label = null;
            this.properties = new HashMap<>();
            this.type = null;
        }

        @Override
        public String toString() {
            return String.format("JsonVertex{label=%s, properties=%s}",
                                 this.label, this.properties);
        }

        public static class Builder {

            private JsonVertex vertex;

            public Builder() {
                this.vertex = new JsonVertex();
            }

            public Builder id(Object id) {
                this.vertex.id = id;
                return this;
            }

            public Builder label(String label) {
                this.vertex.label = label;
                return this;
            }

            public Builder properties(Map<String, Object> properties) {
                this.vertex.properties = properties;
                return this;
            }

            public Builder type(String type) {
                this.vertex.type = type;
                return this;
            }

            public JsonVertex build() {
                E.checkArgumentNotNull(vertex.properties,
                                       "The properties of vertex can't be null");

                for (Map.Entry<String, Object> e :
                     vertex.properties.entrySet()) {
                    String key = e.getKey();
                    Object value = e.getValue();
                    E.checkArgumentNotNull(value, "Not allowed to set value " +
                                           "of property '%s' to null for " +
                                           "vertex '%s'", key, vertex.id);
                }

                return this.vertex;
            }
        }
    }
}
