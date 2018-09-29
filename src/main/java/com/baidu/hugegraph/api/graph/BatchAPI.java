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

package com.baidu.hugegraph.api.graph;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedHashMap;

import com.baidu.hugegraph.client.RestClient;
import com.baidu.hugegraph.exception.NotAllCreatedException;
import com.baidu.hugegraph.rest.RestResult;
import com.baidu.hugegraph.structure.constant.HugeType;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

public class BatchAPI extends GraphAPI {

    public BatchAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return HugeType.BATCH.string();
    }

    @SuppressWarnings("unchecked")
    public Map<String, List<Object>> create(List<Vertex> vertices,
                                            List<Edge> edges,
                                            boolean checkVertex) {
        MultivaluedHashMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.putSingle("Content-Encoding", BATCH_ENCODING);
        Graph body = new Graph(vertices, edges);
        Map<String, Object> params = ImmutableMap.of("check_vertex",
                                                     checkVertex);
        RestResult result = this.client.post(this.path(), body, headers,
                                             params);

        Map<String, Object> results = result.readObject(Map.class);
        List<Object> vertexIds = (List<Object>) results.get("vertices");
        if (vertices.size() != vertexIds.size()) {
            throw new NotAllCreatedException(
                      "Not all vertices are successfully created, " +
                      "expect '%s', the actual is '%s'",
                      vertexIds, vertices.size(), vertexIds.size());
        }
        List<Object> edgeIds = (List<Object>) results.get("edges");
        if (edges.size() != edgeIds.size()) {
            throw new NotAllCreatedException(
                      "Not all edges are successfully created, " +
                      "expect '%s', the actual is '%s'",
                      edgeIds, edges.size(), edgeIds.size());
        }
        return ImmutableMap.of("vertices", vertexIds, "edges", edgeIds);
    }

    public static class Graph {

        @JsonProperty("vertices")
        public List<Vertex> vertices;
        @JsonProperty("edges")
        public List<Edge> edges;

        public Graph(List<Vertex> vertices, List<Edge> edges) {
            this.vertices = vertices;
            this.edges = edges;
        }
    }
}
