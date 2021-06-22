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

package com.baidu.hugegraph.structure.auth;

import java.util.Date;
import java.util.List;
import java.util.Objects;

import com.baidu.hugegraph.structure.constant.HugeType;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Project extends AuthElement {

    @JsonProperty("project_description")
    private String description;
    @JsonProperty("project_target")
    private String target;
    @JsonProperty("project_admin_group")
    private String adminGroup;
    @JsonProperty("project_op_group")
    private String opGroup;
    @JsonProperty("project_name")
    private String name;
    @JsonProperty("project_graphs")
    private List<String> graphs;
    @JsonProperty("project_graph")
    private String graph;

    @JsonProperty("project_create")
    @JsonFormat(pattern = DATE_FORMAT)
    private Date create;
    @JsonProperty("project_creator")
    private String creator;
    @JsonProperty("project_update")
    @JsonFormat(pattern = DATE_FORMAT)
    private Date update;

    public String description() {
        return description;
    }

    public void description(String description) {
        this.description = description;
    }

    public String target() {
        return target;
    }

    public void target(String target) {
        this.target = target;
    }

    public String adminGroup() {
        return adminGroup;
    }

    public void adminGroup(String adminGroup) {
        this.adminGroup = adminGroup;
    }

    public String opGroup() {
        return opGroup;
    }

    public void opGroup(String opGroup) {
        this.opGroup = opGroup;
    }

    public String name() {
        return name;
    }

    public void name(String name) {
        this.name = name;
    }

    public List<String> graphs() {
        return graphs;
    }

    public void graphs(List<String> graphs) {
        this.graphs = graphs;
    }

    public String graph() {
        return graph;
    }

    public void graph(String graph) {
        this.graph = graph;
    }

    @Override
    public String type() {
        return HugeType.PROJECT.string();
    }

    @Override
    public Date createTime() {
        return this.create;
    }

    @Override
    public Date updateTime() {
        return this.update;
    }

    @Override
    public String creator() {
        return this.creator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        Project project = (Project) o;
        return Objects.equals(description, project.description) &&
               Objects.equals(target, project.target) &&
               Objects.equals(adminGroup, project.adminGroup) &&
               Objects.equals(opGroup, project.opGroup) &&
               Objects.equals(name, project.name) &&
               Objects.equals(graphs, project.graphs) &&
               Objects.equals(graph, project.graph) &&
               Objects.equals(create, project.create) &&
               Objects.equals(creator, project.creator) &&
               Objects.equals(update, project.update);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), description, target, adminGroup,
                            opGroup, name, graphs, graph, create, creator,
                            update);
    }
}
