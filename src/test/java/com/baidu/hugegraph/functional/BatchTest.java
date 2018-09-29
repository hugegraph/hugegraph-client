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

package com.baidu.hugegraph.functional;

import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.BaseClientTest;
import com.baidu.hugegraph.exception.ServerException;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.testutil.Assert;

public class BatchTest extends BaseFuncTest {

    @Before
    public void setup() {
        BaseClientTest.initPropertyKey();
        BatchTest.initVertexLabel();
        BatchTest.initEdgeLabel();
    }

    @After
    public void teardown() throws Exception {
        BaseFuncTest.clearData();
    }

    protected static void initVertexLabel() {
        schema().vertexLabel("person")
                .useCustomizeNumberId()
                .properties("name", "age", "price")
                .nullableKeys("price")
                .ifNotExist()
                .create();

        schema().vertexLabel("software")
                .useCustomizeNumberId()
                .properties("name", "lang", "price")
                .ifNotExist()
                .create();
    }

    protected static void initEdgeLabel() {
        schema().edgeLabel("knows")
                .link("person", "person")
                .properties("date")
                .ifNotExist()
                .create();

        schema().edgeLabel("created")
                .link("person", "software")
                .properties("date")
                .ifNotExist()
                .create();
    }

    @Test
    public void testAddVerticesAndEdges() {
        Vertex marko = new Vertex("person").property("name", "marko")
                                           .property("age", 29);
        marko.id(1);
        Vertex vadas = new Vertex("person").property("name", "vadas")
                                           .property("age", 27);
        vadas.id(2);
        Vertex lop = new Vertex("software").property("name", "lop")
                                           .property("lang", "java")
                                           .property("price", 328);
        lop.id(3);
        Vertex josh = new Vertex("person").property("name", "josh")
                                          .property("age", 32);
        josh.id(4);
        Vertex ripple = new Vertex("software").property("name", "ripple")
                                              .property("lang", "java")
                                              .property("price", 199);
        ripple.id(5);
        Vertex peter = new Vertex("person").property("name", "peter")
                                           .property("age", 35);
        peter.id(6);

        List<Vertex> vertices = new LinkedList<>();
        vertices.add(marko);
        vertices.add(vadas);
        vertices.add(lop);
        vertices.add(josh);
        vertices.add(ripple);
        vertices.add(peter);

        Edge markoKnowsVadas = new Edge("knows").source(marko).target(vadas)
                                                .property("date", "20160110");
        Edge markoKnowsJosh = new Edge("knows").source(marko).target(josh)
                                               .property("date", "20130220");
        Edge markoCreateLop = new Edge("created").source(marko).target(lop)
                                                 .property("date", "20171210");
        Edge joshCreateRipple = new Edge("created").source(josh).target(ripple)
                                                   .property("date", "20171210");
        Edge joshCreateLop = new Edge("created").source(josh).target(lop)
                                                .property("date", "20091111");
        Edge peterCreateLop = new Edge("created").source(peter).target(lop)
                                                 .property("date", "20170324");

        List<Edge> edges = new LinkedList<>();
        edges.add(markoKnowsVadas);
        edges.add(markoKnowsJosh);
        edges.add(markoCreateLop);
        edges.add(joshCreateRipple);
        edges.add(joshCreateLop);
        edges.add(peterCreateLop);

        graph().addVerticesAndEdges(vertices, edges);

        Assert.assertEquals(6, graph().listVertices().size());
        Assert.assertEquals(6, graph().listEdges().size());
    }

    @Test
    public void testAddVerticesAndEdgesFailed() {
        Vertex marko = new Vertex("person").property("name", "marko")
                                           .property("age", 29);
        marko.id(1);
        Vertex vadas = new Vertex("person").property("name", "vadas")
                                           .property("age", 27);
        vadas.id(2);
        Vertex lop = new Vertex("software").property("name", "lop")
                                           .property("lang", "java")
                                           .property("price", 328);
        lop.id(3);
        Vertex josh = new Vertex("person").property("name", "josh")
                                          .property("age", 32);
        josh.id(4);
        Vertex ripple = new Vertex("software").property("name", "ripple")
                                              .property("lang", "java")
                                              .property("price", 199);
        ripple.id(5);
        Vertex peter = new Vertex("person").property("name", "peter")
                                           .property("age", 35);
        peter.id(6);

        List<Vertex> vertices = new LinkedList<>();
        vertices.add(marko);
        vertices.add(vadas);
        vertices.add(lop);
        vertices.add(josh);
        vertices.add(ripple);
        vertices.add(peter);

        // Missing nonnullable property 'date'
        Edge markoKnowsVadas = new Edge("knows").source(marko).target(vadas);
        Edge markoKnowsJosh = new Edge("knows").source(marko).target(josh)
                                               .property("date", "20130220");
        Edge markoCreateLop = new Edge("created").source(marko).target(lop)
                                                 .property("date", "20171210");
        Edge joshCreateRipple = new Edge("created").source(josh).target(ripple)
                                                   .property("date", "20171210");
        Edge joshCreateLop = new Edge("created").source(josh).target(lop)
                                                .property("date", "20091111");
        Edge peterCreateLop = new Edge("created").source(peter).target(lop)
                                                 .property("date", "20170324");

        List<Edge> edges = new LinkedList<>();
        edges.add(markoKnowsVadas);
        edges.add(markoKnowsJosh);
        edges.add(markoCreateLop);
        edges.add(joshCreateRipple);
        edges.add(joshCreateLop);
        edges.add(peterCreateLop);

        Assert.assertThrows(ServerException.class, () -> {
            graph().addVerticesAndEdges(vertices, edges);
        });

        Assert.assertEquals(0, graph().listVertices().size());
        Assert.assertEquals(0, graph().listEdges().size());
    }
}
