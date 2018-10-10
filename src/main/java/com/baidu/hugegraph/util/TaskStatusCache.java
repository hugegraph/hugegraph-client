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

package com.baidu.hugegraph.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.baidu.hugegraph.api.task.TaskAPI;
import com.baidu.hugegraph.structure.Task;

public class TaskStatusCache {

    private static final Task FAKE_TASK = new Task();

    private Map<String, Map<Long, Task>> tasks;
    private Map<String, AtomicBoolean> querying;
    private ScheduledExecutorService service;

    private static TaskStatusCache instance = new TaskStatusCache();

    private TaskStatusCache() {
        this.tasks = new ConcurrentHashMap<>();
        this.querying = new HashMap<>();
        this.service = null;
    }

    public static TaskStatusCache instance() {
        return instance;
    }

    public void add(TaskAPI api, long taskId) {
        Map<Long, Task> tasks = this.tasksOfGraph(api.graph());
        tasks.putIfAbsent(taskId, FAKE_TASK);
        this.scheduleQueryTask(api);
    }

    public Task get(TaskAPI api, long taskId) {
        this.add(api, taskId);
        String graph = api.graph();
        while (true) {
            Task task = this.tasks.get(graph).get(taskId);
            if (task != FAKE_TASK) {
                return task;
            }
        }
    }

    public void remove(TaskAPI api, long taskId) {
        Map<Long, Task> tasks = this.tasksOfGraph(api.graph());
        tasks.remove(taskId);
        if (this.requestsEmpty()) {
            this.querying = new HashMap<>();
            this.service.shutdown();
        }
    }

    private void scheduleQueryTask(TaskAPI api) {
        String graph = api.graph();

        // Already start one query task for this graph, ignore
        if (this.queryStatusOfGraph(graph)) {
            return;
        }

        // If no tasks need to query status, no need to start query task
        Map<Long, Task> tasks = this.tasks.get(graph);
        if (tasks == null || tasks.isEmpty()) {
            return;
        }

        // Create new thread pool if first time or prior thread pool shutdown
        if (this.service == null || this.service.isShutdown()) {
            this.service = Executors.newSingleThreadScheduledExecutor();
        }

        Runnable run = () -> {
            Map<Long, Task> targets = this.tasks.get(graph);
            if (targets == null || targets.isEmpty()) {
                return;
            }
            List<Long> taskIds = new ArrayList<>(targets.keySet());
            List<Task> results = api.list(taskIds);
            for (Task task : results) {
                targets.replace(task.id(), task);
            }
        };

        // Schedule a query task for graph to query task status every 1 second,
        // and label it "querying"
        this.service.scheduleAtFixedRate(run, 0L, 1L, TimeUnit.SECONDS);
        this.querying.get(graph).set(true);
    }

    private Map<Long, Task> tasksOfGraph(String graph) {
        Map<Long, Task> tasks = this.tasks.get(graph);
        if (tasks == null) {
            tasks = new ConcurrentHashMap<>();
            this.tasks.putIfAbsent(graph, tasks);
        }
        return this.tasks.get(graph);
    }

    private boolean queryStatusOfGraph(String graph) {
        AtomicBoolean status = this.querying.get(graph);
        if (status == null) {
            status = new AtomicBoolean(false);
            this.querying.putIfAbsent(graph, status);
        }
        return this.querying.get(graph).get();
    }

    private boolean requestsEmpty() {
        if (this.tasks.isEmpty()) {
            return false;
        }
        for (Map<Long, Task> tasks : this.tasks.values()) {
            if (tasks != null && !tasks.isEmpty()) {
                return false;
            }
        }
        return true;
    }
}
