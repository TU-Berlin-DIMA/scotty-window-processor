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
package stream.scotty.sparkconnector.demo;
import java.io.Serializable;

/**
 * User-defined data type representing the input events
 */
public class DemoEvent implements Serializable {
    private Integer key;
    private Integer value;
    private long timestamp;

    public DemoEvent() {
    }

    public DemoEvent(Integer key, Integer value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public Integer getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Integer getKey() {
        return key;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    public void setKey(Integer key) {
        this.key = key;
    }

    @Override
    public String toString() {
        return "DemoEvent{" +
                "value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }
}
