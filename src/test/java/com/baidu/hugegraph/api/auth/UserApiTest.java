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

package com.baidu.hugegraph.api.auth;

import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.exception.ServerException;
import com.baidu.hugegraph.structure.auth.User;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;

public class UserApiTest extends AuthApiTest {

    private static UserAPI api;

    @BeforeClass
    public static void init() {
        api = new UserAPI(initClient(), GRAPH);
    }

    @AfterClass
    public static void clear() {
        List<User> users = api.list(-1);
        for (User user : users) {
            if (user.name().equals("admin")) {
                continue;
            }
            api.delete(user.id());
        }
    }

    @Override
    @After
    public void teardown() {
        clear();
    }

    @Test
    public void testCreate() {
        User user1 = new User();
        user1.name("user1");
        user1.password("p1");
        user1.email("user1@baidu.com");
        user1.phone("123456789");
        user1.avatar("image1.jpg");

        User user2 = new User();
        user2.name("user2");
        user2.password("p2");
        user2.email("user2@baidu.com");
        user2.phone("1357924680");
        user2.avatar("image2.jpg");

        User result1 = api.create(user1);
        User result2 = api.create(user2);

        Assert.assertEquals("user1", result1.name());
        Assert.assertNotEquals("p1", result1.password());
        Assert.assertEquals("user1@baidu.com", result1.email());
        Assert.assertEquals("123456789", result1.phone());
        Assert.assertEquals("image1.jpg", result1.avatar());

        Assert.assertEquals("user2", result2.name());
        Assert.assertNotEquals("p2", result2.password());
        Assert.assertEquals("user2@baidu.com", result2.email());
        Assert.assertEquals("1357924680", result2.phone());
        Assert.assertEquals("image2.jpg", result2.avatar());

        Assert.assertThrows(ServerException.class, () -> {
            api.create(user1);
        }, e -> {
            Assert.assertContains("Can't save user", e.getMessage());
            Assert.assertContains("that already exists", e.getMessage());
        });

        Assert.assertThrows(ServerException.class, () -> {
            user1.name("admin");
            api.create(user1);
        }, e -> {
            Assert.assertContains("Invalid user name 'admin'", e.getMessage());
        });
    }

    @Test
    public void testGet() {
        User user1 = createUser("test1", "psw1");
        User user2 = createUser("test2", "psw2");

        Assert.assertEquals("test1", user1.name());
        Assert.assertEquals("test2", user2.name());

        user1 = api.get(user1.id());
        user2 = api.get(user2.id());

        Assert.assertEquals("test1", user1.name());
        Assert.assertEquals("test2", user2.name());
    }

    @Test
    public void testList() {
        createUser("test1", "psw1");
        createUser("test2", "psw2");
        createUser("test3", "psw3");

        List<User> users = api.list(-1);
        Assert.assertEquals(4, users.size());

        users.sort((t1, t2) -> t1.name().compareTo(t2.name()));
        Assert.assertEquals("admin", users.get(0).name());
        Assert.assertEquals("test1", users.get(1).name());
        Assert.assertEquals("test2", users.get(2).name());
        Assert.assertEquals("test3", users.get(3).name());

        users = api.list(1);
        Assert.assertEquals(1, users.size());

        users = api.list(2);
        Assert.assertEquals(2, users.size());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            api.list(0);
        }, e -> {
            Assert.assertContains("Limit must be > 0 or == -1", e.getMessage());
        });
    }

    @Test
    public void testUpdate() {
        User user1 = createUser("test1", "psw1");
        User user2 = createUser("test2", "psw2");

        Assert.assertEquals("test@baidu.com", user1.email());
        Assert.assertEquals("16812345678", user1.phone());
        Assert.assertEquals("image.jpg", user1.avatar());

        String oldPassw = user1.password();
        Assert.assertNotEquals("psw1", oldPassw);

        user1.password("psw-udated");
        user1.email("test_updated@baidu.com");
        user1.phone("1357924680");
        user1.avatar("image-updated.jpg");

        User updated = api.update(user1);
        Assert.assertNotEquals(oldPassw, updated.password());
        Assert.assertEquals("test_updated@baidu.com", updated.email());
        Assert.assertEquals("1357924680", updated.phone());
        Assert.assertEquals("image-updated.jpg", updated.avatar());
        Assert.assertNotEquals(user1.updateTime(), updated.updateTime());

        Assert.assertThrows(ServerException.class, () -> {
            user2.name("test2-updated");
            api.update(user2);
        }, e -> {
            Assert.assertContains("The name of user can't be updated",
                                  e.getMessage());
        });

        Assert.assertThrows(ServerException.class, () -> {
            Whitebox.setInternalState(user2, "id", "fake-id");
            api.update(user2);
        }, e -> {
            Assert.assertContains("Invalid user id: fake-id",
                                  e.getMessage());
        });
    }

    @Test
    public void testDelete() {
        User user1 = createUser("test1", "psw1");
        User user2 = createUser("test2", "psw2");

        Assert.assertEquals(3, api.list(-1).size());
        api.delete(user1.id());

        Assert.assertEquals(2, api.list(-1).size());
        Assert.assertEquals(user2, api.list(-1).get(0));

        api.delete(user2.id());
        List<User> users = api.list(-1);
        Assert.assertEquals(1, users.size());
        Assert.assertEquals("admin", users.get(0).name());

        Assert.assertThrows(ServerException.class, () -> {
            api.delete(users.get(0).id());
        }, e -> {
            Assert.assertContains("Can't delete user 'admin'", e.getMessage());
        });

        Assert.assertThrows(ServerException.class, () -> {
            api.delete(user2.id());
        }, e -> {
            Assert.assertContains("Invalid user id:", e.getMessage());
        });

        Assert.assertThrows(ServerException.class, () -> {
            api.delete("fake-id");
        }, e -> {
            Assert.assertContains("Invalid user id: fake-id", e.getMessage());
        });
    }

    protected static User createUser(String name, String password) {
        User user = new User();
        user.name(name);
        user.password(password);
        user.email("test@baidu.com");
        user.phone("16812345678");
        user.avatar("image.jpg");
        return api.create(user);
    }
}
