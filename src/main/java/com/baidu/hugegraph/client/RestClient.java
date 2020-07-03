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

package com.baidu.hugegraph.client;

import javax.ws.rs.core.Response;

import com.baidu.hugegraph.exception.ServerException;
import com.baidu.hugegraph.rest.AbstractRestClient;
import com.baidu.hugegraph.rest.ClientException;
import com.baidu.hugegraph.rest.RestResult;
import com.baidu.hugegraph.serializer.PathDeserializer;
import com.baidu.hugegraph.structure.graph.Path;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.VersionUtil;
import com.baidu.hugegraph.util.VersionUtil.Version;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class RestClient extends AbstractRestClient {

    private static final int SECOND = 1000;

    private Version apiVersion = null;

    static {
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Path.class, new PathDeserializer());
        RestResult.registerModule(module);
    }

    public RestClient(String url, String username, String password,
                      int timeout) {
        super(url, username, password, timeout * SECOND);
    }

    public RestClient(String url, String username, String password, int timeout,
                      int maxConns, int maxConnsPerRoute, String protocol,
                      String trustStoreFile, String trustStorePassword) {
        super(url, username, password, timeout * SECOND, maxConns,
              maxConnsPerRoute, protocol, trustStoreFile, trustStorePassword);
    }

    public void apiVersion(Version version) {
        E.checkNotNull(version, "api version");
        this.apiVersion = version;
    }

    public Version apiVersion() {
        return this.apiVersion;
    }

    public void checkApiVersion(String minVersion, String message) {
        if (this.apiVersionLt(minVersion)) {
            throw new ClientException(
                      "HugeGraphServer API version must be >= %s to support " +
                      "%s, but current HugeGraphServer API version is: %s",
                      minVersion, message, this.apiVersion.get());
        }
    }

    public boolean apiVersionLt(String minVersion) {
        String apiVersion = this.apiVersion == null ?
                            null : this.apiVersion.get();
        return apiVersion != null && !VersionUtil.gte(apiVersion, minVersion);
    }

    @Override
    protected void checkStatus(Response response, Response.Status... statuses) {
        boolean match = false;
        for (Response.Status status : statuses) {
            if (status.getStatusCode() == response.getStatus()) {
                match = true;
                break;
            }
        }
        if (!match) {
            throw ServerException.fromResponse(response);
        }
    }
}
