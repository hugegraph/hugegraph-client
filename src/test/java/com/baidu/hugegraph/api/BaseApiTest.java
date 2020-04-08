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

package com.baidu.hugegraph.api;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.baidu.hugegraph.BaseClientTest;
import com.baidu.hugegraph.api.graph.EdgeAPI;
import com.baidu.hugegraph.api.graph.VertexAPI;
import com.baidu.hugegraph.api.graphs.GraphsAPI;
import com.baidu.hugegraph.api.job.RebuildAPI;
import com.baidu.hugegraph.api.schema.EdgeLabelAPI;
import com.baidu.hugegraph.api.schema.IndexLabelAPI;
import com.baidu.hugegraph.api.schema.PropertyKeyAPI;
import com.baidu.hugegraph.api.schema.VertexLabelAPI;
import com.baidu.hugegraph.api.task.TaskAPI;
import com.baidu.hugegraph.api.traverser.AllShortestPathsAPI;
import com.baidu.hugegraph.api.traverser.CrosspointsAPI;
import com.baidu.hugegraph.api.traverser.CustomizedCrosspointsAPI;
import com.baidu.hugegraph.api.traverser.CustomizedPathsAPI;
import com.baidu.hugegraph.api.traverser.EdgesAPI;
import com.baidu.hugegraph.api.traverser.FusiformSimilarityAPI;
import com.baidu.hugegraph.api.traverser.JaccardSimilarityAPI;
import com.baidu.hugegraph.api.traverser.KneighborAPI;
import com.baidu.hugegraph.api.traverser.KoutAPI;
import com.baidu.hugegraph.api.traverser.NeighborRankAPI;
import com.baidu.hugegraph.api.traverser.PathsAPI;
import com.baidu.hugegraph.api.traverser.PersonalRankAPI;
import com.baidu.hugegraph.api.traverser.RaysAPI;
import com.baidu.hugegraph.api.traverser.RingsAPI;
import com.baidu.hugegraph.api.traverser.SameNeighborsAPI;
import com.baidu.hugegraph.api.traverser.ShortestPathAPI;
import com.baidu.hugegraph.api.traverser.SingleSourceShortestPathAPI;
import com.baidu.hugegraph.api.traverser.VerticesAPI;
import com.baidu.hugegraph.api.traverser.WeightedShortestPathAPI;
import com.baidu.hugegraph.api.variables.VariablesAPI;
import com.baidu.hugegraph.api.version.VersionAPI;
import com.baidu.hugegraph.client.RestClient;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.VersionUtil;

public class BaseApiTest extends BaseClientTest {

    private static RestClient client;

    protected static VersionAPI versionAPI;
    protected static PropertyKeyAPI propertyKeyAPI;
    protected static VertexLabelAPI vertexLabelAPI;
    protected static EdgeLabelAPI edgeLabelAPI;
    protected static IndexLabelAPI indexLabelAPI;
    protected static VertexAPI vertexAPI;
    protected static EdgeAPI edgeAPI;
    protected static VariablesAPI variablesAPI;
    protected static SameNeighborsAPI sameNeighborsAPI;
    protected static JaccardSimilarityAPI jaccardSimilarityAPI;
    protected static ShortestPathAPI shortestPathAPI;
    protected static AllShortestPathsAPI allShortestPathsAPI;
    protected static SingleSourceShortestPathAPI singleSourceShortestPathAPI;
    protected static WeightedShortestPathAPI weightedShortestPathAPI;
    protected static PathsAPI pathsAPI;
    protected static CrosspointsAPI crosspointsAPI;
    protected static KoutAPI koutAPI;
    protected static KneighborAPI kneighborAPI;
    protected static RingsAPI ringsAPI;
    protected static RaysAPI raysAPI;
    protected static CustomizedPathsAPI customizedPathsAPI;
    protected static CustomizedCrosspointsAPI customizedCrosspointsAPI;
    protected static FusiformSimilarityAPI fusiformSimilarityAPI;
    protected static NeighborRankAPI neighborRankAPI;
    protected static PersonalRankAPI personalRankAPI;
    protected static VerticesAPI verticesAPI;
    protected static EdgesAPI edgesAPI;
    protected static TaskAPI taskAPI;
    protected static RebuildAPI rebuildAPI;
    protected static GraphsAPI graphsAPI;

    @BeforeClass
    public static void init() {
        BaseClientTest.init();

        client = new RestClient(BASE_URL, 5);
        versionAPI = new VersionAPI(client);
        client.apiVersion(VersionUtil.Version.of(versionAPI.get().get("api")));

        propertyKeyAPI = new PropertyKeyAPI(client, GRAPH);
        vertexLabelAPI = new VertexLabelAPI(client, GRAPH);
        edgeLabelAPI = new EdgeLabelAPI(client, GRAPH);
        indexLabelAPI = new IndexLabelAPI(client, GRAPH);
        vertexAPI = new VertexAPI(client, GRAPH);
        edgeAPI = new EdgeAPI(client, GRAPH);
        variablesAPI = new VariablesAPI(client, GRAPH);
        sameNeighborsAPI = new SameNeighborsAPI(client, GRAPH);
        jaccardSimilarityAPI = new JaccardSimilarityAPI(client, GRAPH);
        shortestPathAPI = new ShortestPathAPI(client, GRAPH);
        allShortestPathsAPI = new AllShortestPathsAPI(client, GRAPH);
        singleSourceShortestPathAPI = new SingleSourceShortestPathAPI(client,
                                                                      GRAPH);
        weightedShortestPathAPI = new WeightedShortestPathAPI(client, GRAPH);
        pathsAPI = new PathsAPI(client, GRAPH);
        crosspointsAPI = new CrosspointsAPI(client, GRAPH);
        koutAPI = new KoutAPI(client, GRAPH);
        kneighborAPI = new KneighborAPI(client, GRAPH);
        ringsAPI = new RingsAPI(client, GRAPH);
        raysAPI = new RaysAPI(client, GRAPH);
        customizedPathsAPI = new CustomizedPathsAPI(client, GRAPH);
        customizedCrosspointsAPI = new CustomizedCrosspointsAPI(client, GRAPH);
        fusiformSimilarityAPI = new FusiformSimilarityAPI(client, GRAPH);
        neighborRankAPI = new NeighborRankAPI(client, GRAPH);
        personalRankAPI = new PersonalRankAPI(client, GRAPH);
        verticesAPI = new VerticesAPI(client, GRAPH);
        edgesAPI = new EdgesAPI(client, GRAPH);
        taskAPI = new TaskAPI(client, GRAPH);
        rebuildAPI = new RebuildAPI(client, GRAPH);
        graphsAPI = new GraphsAPI(client);
    }

    @AfterClass
    public static void clear() throws Exception {
        Assert.assertNotNull("Not opened client", client);

        clearData();
        client.close();

        BaseClientTest.clear();
    }

    protected RestClient client() {
        return client;
    }

    protected static void clearData() {
        // Clear edge
        edgeAPI.list(-1).results().forEach(edge -> {
            edgeAPI.delete(edge.id());
        });
        // Clear vertex
        vertexAPI.list(-1).results().forEach(vertex -> {
            vertexAPI.delete(vertex.id());
        });

        // Clear schema
        List<Long> ilTaskIds = new ArrayList<>();
        indexLabelAPI.list().forEach(indexLabel -> {
            ilTaskIds.add(indexLabelAPI.delete(indexLabel.name()));
        });
        ilTaskIds.forEach(taskId -> waitUntilTaskCompleted(taskId));

        List<Long> elTaskIds = new ArrayList<>();
        edgeLabelAPI.list().forEach(edgeLabel -> {
            elTaskIds.add(edgeLabelAPI.delete(edgeLabel.name()));
        });
        elTaskIds.forEach(taskId -> waitUntilTaskCompleted(taskId));

        List<Long> vlTaskIds = new ArrayList<>();
        vertexLabelAPI.list().forEach(vertexLabel -> {
            vlTaskIds.add(vertexLabelAPI.delete(vertexLabel.name()));
        });
        vlTaskIds.forEach(taskId -> waitUntilTaskCompleted(taskId));

        propertyKeyAPI.list().forEach(propertyKey -> {
            propertyKeyAPI.delete(propertyKey.name());
        });

        // Clear system
        taskAPI.list(null, -1).forEach(task -> {
            taskAPI.delete(task.id());
        });
    }

    protected static void waitUntilTaskCompleted(long taskId) {
        taskAPI.waitUntilTaskSuccess(taskId, 5L);
    }

    protected static void waitUntilTaskCompleted(long taskId, long timeout) {
        taskAPI.waitUntilTaskSuccess(taskId, timeout);
    }
}
