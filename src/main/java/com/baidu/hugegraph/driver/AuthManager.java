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

import com.baidu.hugegraph.api.auth.AccessAPI;
import com.baidu.hugegraph.api.auth.BelongAPI;
import com.baidu.hugegraph.api.auth.GroupAPI;
import com.baidu.hugegraph.api.auth.TargetAPI;
import com.baidu.hugegraph.api.auth.UserAPI;
import com.baidu.hugegraph.client.RestClient;
import com.baidu.hugegraph.structure.auth.Access;
import com.baidu.hugegraph.structure.auth.Belong;
import com.baidu.hugegraph.structure.auth.Group;
import com.baidu.hugegraph.structure.auth.Target;
import com.baidu.hugegraph.structure.auth.User;

public class AuthManager {

    private TargetAPI targetAPI;
    private GroupAPI groupAPI;
    private UserAPI userAPI;
    private AccessAPI accessAPI;
    private BelongAPI belongAPI;

    public AuthManager(RestClient client, String graph) {
        this.targetAPI = new TargetAPI(client, graph);
        this.groupAPI = new GroupAPI(client, graph);
        this.userAPI = new UserAPI(client, graph);
        this.accessAPI = new AccessAPI(client, graph);
        this.belongAPI = new BelongAPI(client, graph);
    }

    public List<Target> listTargets() {
        return this.listTargets(-1);
    }

    public List<Target> listTargets(int limit) {
        return this.targetAPI.list(limit);
    }

    public Target getTarget(Object id) {
        return this.targetAPI.get(id);
    }

    public void createTarget(Target target) {
        this.targetAPI.create(target);
    }

    public void updateTarget(Target target) {
        this.targetAPI.update(target);
    }

    public void deleteTarget(Object id) {
        this.targetAPI.delete(id);
    }

    public List<Group> listGroups() {
        return this.listGroups(-1);
    }

    public List<Group> listGroups(int limit) {
        return this.groupAPI.list(limit);
    }

    public Group getGroup(Object id) {
        return this.groupAPI.get(id);
    }

    public void createGroup(Group group) {
        this.groupAPI.create(group);
    }

    public void updateGroup(Group group) {
        this.groupAPI.update(group);
    }

    public void deleteGroup(Object id) {
        this.groupAPI.delete(id);
    }

    public List<User> listUsers() {
        return this.listUsers(-1);
    }

    public List<User> listUsers(int limit) {
        return this.userAPI.list(limit);
    }

    public User getUser(Object id) {
        return this.userAPI.get(id);
    }

    public void createUser(User user) {
        this.userAPI.create(user);
    }

    public void updateUser(User user) {
        this.userAPI.update(user);
    }

    public void deleteUser(Object id) {
        this.userAPI.delete(id);
    }

    public List<Access> listAccesses() {
        return this.listAccesses(-1);
    }

    public List<Access> listAccesses(int limit) {
        return this.accessAPI.list(null, null, limit);
    }

    public List<Access> listAccessesByGroup(Object group, int limit) {
        return this.accessAPI.list(group, null, limit);
    }

    public List<Access> listAccessesByTarget(Object target, int limit) {
        return this.accessAPI.list(null, target, limit);
    }

    public Access getAccess(Object id) {
        return this.accessAPI.get(id);
    }

    public void createAccess(Access access) {
        this.accessAPI.create(access);
    }

    public void updateAccess(Access access) {
        this.accessAPI.update(access);
    }

    public void deleteAccess(Object id) {
        this.accessAPI.delete(id);
    }

    public List<Belong> listBelongs() {
        return this.listBelongs(-1);
    }

    public List<Belong> listBelongs(int limit) {
        return this.belongAPI.list(null, null, limit);
    }

    public List<Belong> listBelongsByUser(Object user, int limit) {
        return this.belongAPI.list(user, null, limit);
    }

    public List<Belong> listBelongsByGroup(Object group, int limit) {
        return this.belongAPI.list(null, group, limit);
    }

    public Belong getBelong(Object id) {
        return this.belongAPI.get(id);
    }

    public void createBelong(Belong belong) {
        this.belongAPI.create(belong);
    }

    public void updateBelong(Belong belong) {
        this.belongAPI.update(belong);
    }

    public void deleteBelong(Object id) {
        this.belongAPI.delete(id);
    }
}
