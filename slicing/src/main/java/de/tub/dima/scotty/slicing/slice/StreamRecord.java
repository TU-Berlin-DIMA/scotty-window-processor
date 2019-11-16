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
package de.tub.dima.scotty.slicing.slice;

import org.jetbrains.annotations.*;

public class StreamRecord<Type> implements Comparable<StreamRecord<Type>> {
    public final long ts;
    public final Type record;

    public StreamRecord(long ts, Type type) {
        this.ts = ts;
        this.record = type;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof StreamRecord) {
            if (((StreamRecord) obj).record.equals(this.record)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int compareTo(@NotNull StreamRecord<Type> o) {
        return Long.compare(ts, o.ts);
    }

    @Override
    public String toString() {
        return "(ts="+ ts + ", value=" + record +")";
    }
}
