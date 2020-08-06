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

package com.baidu.hugegraph.api.schema;

import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.client.RestClient;
import com.baidu.hugegraph.rest.RestResult;
import com.baidu.hugegraph.structure.SchemaElement;
import com.baidu.hugegraph.structure.constant.HugeType;
import com.baidu.hugegraph.structure.schema.PropertyKey;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableMap;

public class PropertyKeyAPI extends SchemaAPI {

    public PropertyKeyAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return HugeType.PROPERTY_KEY.string();
    }

    public PropertyKey create(PropertyKey propertyKey) {
        Object pkey = this.checkCreateOrUpdate(propertyKey);
        RestResult result = this.client.post(this.path(), pkey);
        return result.readObject(PropertyKey.class);
    }

    public PropertyKey append(PropertyKey propertyKey) {
        String id = propertyKey.name();
        Map<String, Object> params = ImmutableMap.of("action", "append");
        Object pkey = this.checkCreateOrUpdate(propertyKey);
        RestResult result = this.client.put(this.path(), id, pkey, params);
        return result.readObject(PropertyKey.class);
    }

    public PropertyKey eliminate(PropertyKey propertyKey) {
        String id = propertyKey.name();
        Map<String, Object> params = ImmutableMap.of("action", "eliminate");
        Object pkey = this.checkCreateOrUpdate(propertyKey);
        RestResult result = this.client.put(this.path(), id, pkey, params);
        return result.readObject(PropertyKey.class);
    }

    public PropertyKey get(String name) {
        RestResult result = this.client.get(this.path(), name);
        return result.readObject(PropertyKey.class);
    }

    public List<PropertyKey> list() {
        RestResult result = this.client.get(this.path());
        return result.readList(this.type(), PropertyKey.class);
    }

    public List<PropertyKey> list(List<String> names) {
        this.client.checkApiVersion("0.48", "getting schema by names");
        E.checkArgument(names != null && !names.isEmpty(),
                        "The property key names can't be null or empty");
        Map<String, Object> params = ImmutableMap.of("names", names);
        RestResult result = this.client.get(this.path(), params);
        return result.readList(this.type(), PropertyKey.class);
    }

    public void delete(String name) {
        this.client.delete(this.path(), name);
    }

    @Override
    protected Object checkCreateOrUpdate(SchemaElement schemaElement) {
        PropertyKey propertyKey = (PropertyKey) schemaElement;
        Object pkey = propertyKey;
        if (this.client.apiVersionLt("0.47")) {
            E.checkArgument(propertyKey.aggregateType().isNone(),
                            "Not support aggregate property until " +
                            "api version 0.47");
            pkey = propertyKey.switchV46();
        }
        return pkey;
    }
}
