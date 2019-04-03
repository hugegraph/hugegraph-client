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

import java.util.List;

import com.baidu.hugegraph.api.traverser.CrosspointsAPI;
import com.baidu.hugegraph.api.traverser.CustomizedCrosspointsAPI;
import com.baidu.hugegraph.api.traverser.CustomizedPathsAPI;
import com.baidu.hugegraph.api.traverser.EdgesAPI;
import com.baidu.hugegraph.api.traverser.KneighborAPI;
import com.baidu.hugegraph.api.traverser.KoutAPI;
import com.baidu.hugegraph.api.traverser.PathsAPI;
import com.baidu.hugegraph.api.traverser.RaysAPI;
import com.baidu.hugegraph.api.traverser.RingsAPI;
import com.baidu.hugegraph.api.traverser.ShortestPathAPI;
import com.baidu.hugegraph.api.traverser.VerticesAPI;
import com.baidu.hugegraph.api.traverser.structure.CrosspointsRequest;
import com.baidu.hugegraph.api.traverser.structure.CustomizedCrosspoints;
import com.baidu.hugegraph.api.traverser.structure.CustomizedPaths;
import com.baidu.hugegraph.api.traverser.structure.PathsRequest;
import com.baidu.hugegraph.client.RestClient;
import com.baidu.hugegraph.structure.constant.Direction;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Edges;
import com.baidu.hugegraph.structure.graph.Path;
import com.baidu.hugegraph.structure.graph.Shard;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.structure.graph.Vertices;
import com.baidu.hugegraph.util.E;

import static com.baidu.hugegraph.structure.constant.Traverser.DEFAULT_PAGE_LIMIT;

public class TraverserManager {

    private final GraphManager graphManager;

    private ShortestPathAPI shortestPathAPI;
    private PathsAPI pathsAPI;
    private CrosspointsAPI crosspointsAPI;
    private KoutAPI koutAPI;
    private KneighborAPI kneighborAPI;
    private RingsAPI ringsAPI;
    private RaysAPI raysAPI;
    private CustomizedPathsAPI customizedPathsAPI;
    private CustomizedCrosspointsAPI customizedCrosspointsAPI;
    private VerticesAPI verticesAPI;
    private EdgesAPI edgesAPI;

    public TraverserManager(RestClient client, GraphManager graphManager) {
        this.graphManager = graphManager;
        String graph = graphManager.graph();
        this.shortestPathAPI = new ShortestPathAPI(client, graph);
        this.pathsAPI = new PathsAPI(client, graph);
        this.crosspointsAPI = new CrosspointsAPI(client, graph);
        this.koutAPI = new KoutAPI(client, graph);
        this.kneighborAPI = new KneighborAPI(client, graph);
        this.ringsAPI = new RingsAPI(client, graph);
        this.raysAPI = new RaysAPI(client, graph);
        this.customizedPathsAPI = new CustomizedPathsAPI(client, graph);
        this.customizedCrosspointsAPI = new CustomizedCrosspointsAPI(client,
                                                                     graph);
        this.verticesAPI = new VerticesAPI(client, graph);
        this.edgesAPI = new EdgesAPI(client, graph);
    }

    public Path shortestPath(Object sourceId, Object targetId,
                             Direction direction, int maxDepth) {
        return this.shortestPath(sourceId, targetId, direction, null,
                                 maxDepth);
    }

    public Path shortestPath(Object sourceId, Object targetId,
                             Direction direction, String label, int maxDepth) {
        return this.shortestPath(sourceId, targetId, direction,
                                 label, maxDepth, -1L, -1L);
    }

    public Path shortestPath(Object sourceId, Object targetId,
                             Direction direction, String label, int maxDepth,
                             long degree, long capacity) {
        return this.shortestPathAPI.get(sourceId, targetId, direction,
                                        label, maxDepth, degree, capacity);
    }

    public List<Path> paths(Object sourceId, Object targetId,
                            Direction direction, int maxDepth, long limit) {
        return this.paths(sourceId, targetId, direction, null,
                          maxDepth, limit);
    }

    public List<Path> paths(Object sourceId, Object targetId,
                            Direction direction, String label,
                            int maxDepth, long limit) {
        return this.paths(sourceId, targetId, direction,
                          label, maxDepth, -1L, -1L, limit);
    }

    public List<Path> paths(Object sourceId, Object targetId,
                            Direction direction, String label, int maxDepth,
                            long degree, long capacity, long limit) {
        return this.pathsAPI.get(sourceId, targetId, direction, label,
                                 maxDepth, degree, capacity, limit);
    }

    public List<Path> crosspoint(Object sourceId, Object targetId,
                                 Direction direction, int maxDepth, int limit) {
        return this.crosspoint(sourceId, targetId, direction, null,
                               maxDepth, limit);
    }

    public List<Path> crosspoint(Object sourceId, Object targetId,
                                 Direction direction, String label,
                                 int maxDepth, int limit) {
        return this.crosspoint(sourceId, targetId, direction,
                               label, maxDepth, -1L, -1L, limit);
    }

    public List<Path> crosspoint(Object sourceId, Object targetId,
                                 Direction direction, String label,
                                 int maxDepth, long degree, long capacity,
                                 long limit) {
        return this.crosspointsAPI.get(sourceId, targetId, direction, label,
                                       maxDepth, degree, capacity, limit);
    }

    public List<Object> kout(Object sourceId, Direction direction, int depth) {
        return this.kout(sourceId, direction, null, depth, true);
    }

    public List<Object> kout(Object sourceId, Direction direction,
                             String label, int depth, boolean nearest) {
        return this.kout(sourceId, direction, label, depth, nearest,
                         -1L, -1L, -1L);
    }

    public List<Object> kout(Object sourceId, Direction direction,
                             String label, int depth, boolean nearest,
                             long degree, long capacity, long limit) {
        return this.koutAPI.get(sourceId, direction, label, depth, nearest,
                                degree, capacity, limit);
    }

    public List<Object> kneighbor(Object sourceId, Direction direction,
                                  int depth) {
        return this.kneighbor(sourceId, direction, null, depth);
    }

    public List<Object> kneighbor(Object sourceId, Direction direction,
                                  String label, int depth) {
        return this.kneighbor(sourceId, direction, label, depth, -1L, -1L);
    }

    public List<Object> kneighbor(Object sourceId, Direction direction,
                                  String label, int depth,
                                  long degree, long limit) {
        return this.kneighborAPI.get(sourceId, direction, label, depth,
                                     degree, limit);
    }

    public List<Path> rings(Object sourceId, Direction direction, String label,
                            int depth, long degree, long capacity, long limit) {
        return this.ringsAPI.get(sourceId, direction, label, depth, degree,
                                 capacity, limit);
    }

    public List<Path> rays(Object sourceId, Direction direction, String label,
                           int depth, long degree, long capacity, long limit) {
        return this.raysAPI.get(sourceId, direction, label, depth, degree,
                                capacity, limit);
    }

    public CustomizedPaths customizedPaths(PathsRequest request) {
        return this.customizedPathsAPI.post(request);
    }

    public CustomizedCrosspoints customizedCrosspointss(
                                 CrosspointsRequest request) {
        return this.customizedCrosspointsAPI.post(request);
    }

    public List<Shard> vertexShards(long splitSize) {
        return this.verticesAPI.shards(splitSize);
    }

    public List<Shard> edgeShards(long splitSize) {
        return this.edgesAPI.shards(splitSize);
    }

    public List<Vertex> vertices(List<Object> ids) {
        List<Vertex> vertices = this.verticesAPI.list(ids);
        for (Vertex vertex : vertices) {
            vertex.attachManager(this.graphManager);
        }
        return vertices;
    }

    public Vertices vertices(Shard shard) {
        Vertices vertices = this.vertices(shard, null, 0L);
        E.checkState(vertices.page() == null,
                     "Can't contains page when not in paging");
        return vertices;
    }

    public Vertices vertices(Shard shard, String page) {
        E.checkArgument(page != null, "Page can't be null");
        return this.vertices(shard, page, DEFAULT_PAGE_LIMIT);
    }

    public Vertices vertices(Shard shard, String page, long pageLimit) {
        E.checkArgument(page == null || pageLimit >= 0,
                        "Page limit must be >= 0 when page is not null");
        Vertices vertices = this.verticesAPI.scan(shard, page, pageLimit);

        for (Vertex vertex : vertices.results()) {
            vertex.attachManager(this.graphManager);
        }
        return vertices;
    }

    public List<Edge> edges(List<String> ids) {
        List<Edge> edges = this.edgesAPI.list(ids);
        for (Edge edge : edges) {
            edge.attachManager(this.graphManager);
        }
        return edges;
    }

    public Edges edges(Shard shard) {
        Edges edges = this.edges(shard, null, 0L);
        E.checkState(edges.page() == null,
                     "Can't contains page when not in paging");
        return edges;
    }

    public Edges edges(Shard shard, String page) {
        E.checkArgument(page != null, "Page can't be null");
        return this.edges(shard, page, DEFAULT_PAGE_LIMIT);
    }

    public Edges edges(Shard shard, String page, long pageLimit) {
        E.checkArgument(page == null || pageLimit >= 0,
                        "Page limit must be >= 0 when page is not null");
        Edges edges = this.edgesAPI.scan(shard, page, pageLimit);
        for (Edge edge : edges.results()) {
            edge.attachManager(this.graphManager);
        }
        return edges;
    }
}
