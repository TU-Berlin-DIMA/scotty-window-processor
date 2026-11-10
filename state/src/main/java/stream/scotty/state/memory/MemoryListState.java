/*
 * Copyright The Scotty Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package stream.scotty.state.memory;

import stream.scotty.state.ListState;

import java.util.ArrayList;
import java.util.List;

public class MemoryListState<T> implements ListState<T> {

    private ArrayList<T> innerList = new ArrayList<>();

    @Override
    public List<T> get() {
        return innerList;
    }

    @Override
    public void set(int i, T value) {
        innerList.set(i, value);
    }

    @Override
    public void add(int i, T value) {
        innerList.add(i, value);
    }

    @Override
    public void clean() {
        innerList.clear();
    }

    @Override
    public boolean isEmpty() {
        return innerList.isEmpty();
    }
}
