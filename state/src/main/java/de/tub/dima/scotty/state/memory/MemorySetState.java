/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.tub.dima.scotty.state.memory;

import de.tub.dima.scotty.state.*;

import java.util.*;

public class MemorySetState<Type extends Comparable<Type>> implements SetState<Type> {

    private TreeSet<Type> values = new TreeSet<>();

    public Type getLast() {
        return values.last();
    }

    public Type getFirst() {
        return values.first();
    }

    public Type dropLast() {
        return values.pollLast();
    }

    public Type dropFrist() {
        return values.pollFirst();
    }

    public void add(Type record) {
        values.add(record);
    }

    @Override
    public Iterator<Type> iterator() {
        return values.iterator();
    }

    @Override
    public void clean() {
        values.clear();
    }

    @Override
    public boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public String toString() {
        return values.toString();
    }
}