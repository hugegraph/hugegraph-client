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

package com.baidu.hugegraph.api.task;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.client.RestClient;
import com.baidu.hugegraph.rest.ClientException;
import com.baidu.hugegraph.rest.RestResult;
import com.baidu.hugegraph.structure.Task;
import com.baidu.hugegraph.structure.constant.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.TaskStatusCache;

public class TaskAPI extends API {

    private static final String PATH = "graphs/%s/tasks";
    private String graph;
    public static final String TASKS = "tasks";
    public static final String TASK_ID_KEY = "task_id";

    public TaskAPI(RestClient client, String graph) {
        super(client);
        this.path(String.format(PATH, graph));
        this.graph = graph;
    }

    @Override
    protected String type() {
        return HugeType.TASK.string();
    }

    public String graph() {
        return this.graph;
    }

    public List<Task> list(String status, long limit) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("limit", limit);
        if (status != null) {
            params.put("status", status);
        }
        RestResult result = this.client.get(this.path(), params);
        return result.readList(TASKS, Task.class);
    }

    public List<Task> list(List<Long> ids) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("ids", ids);
        RestResult result = this.client.get(this.path(), params);
        return result.readList(TASKS, Task.class);
    }

    public Task get(long id) {
        RestResult result = this.client.get(this.path(), String.valueOf(id));
        return result.readObject(Task.class);
    }

    public void delete(long id) {
        this.client.delete(path(), String.valueOf(id));
    }

    public void waitUntilTaskCompleted(long taskId, long seconds) {
        for (long pass = 0;; pass++) {
            Task task = this.getFromCache(taskId);
            if (this.completed(task)) {
                return;
            }
            if (pass >= seconds) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // Ignore
            }
        }
        throw new ClientException("Task '%s' was not completed in %s seconds",
                                  taskId, seconds);
    }

    private boolean completed(Task task) {
        if (task.completed() && !task.success()) {
            throw new ClientException("Task '%s' is '%s', result is '%s'",
                      task.id(), task.status(), task.result());
        }
        if (!task.completed()) {
            return false;
        }
        if (task.children() == null || task.children().isEmpty()) {
            this.removeFromCache(task.id());
            return true;
        }
        for (long id : task.children()) {
            Task child = this.getFromCache(id);
            if (!this.completed(child)) {
                return false;
            }
        }
        this.removeFromCache(task.id());
        return true;
    }

    private Task getFromCache(long taskId) {
        return TaskStatusCache.instance().get(this, taskId);
    }

    private void removeFromCache(long taskId) {
        TaskStatusCache.instance().remove(this, taskId);
    }

    public static long parseTaskId(Map<String, Object> task) {
        E.checkState(task.size() == 1 && task.containsKey(TASK_ID_KEY),
                     "Task must be formatted to {\"%s\" : id}, but got %s",
                     TASK_ID_KEY, task);
        Object taskId = task.get(TASK_ID_KEY);
        E.checkState(taskId instanceof Number,
                     "Task id must be number, but got '%s'", taskId);
        return ((Number) taskId).longValue();
    }
}
