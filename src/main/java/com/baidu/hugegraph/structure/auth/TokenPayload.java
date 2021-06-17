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

import java.util.Map;

public class TokenPayload {

    private String userId;
    private String username;

    private TokenPayload() {
    }

    public String userId() {
        return this.userId;
    }

    public String username() {
        return this.username;
    }

    public static TokenPayload fromMap(Map<String, Object> map) {
        TokenPayload payload = new TokenPayload();
        payload.username = (String) map.get("user_name");
        payload.userId = (String) map.get("user_id");
        return payload;
    }
}
