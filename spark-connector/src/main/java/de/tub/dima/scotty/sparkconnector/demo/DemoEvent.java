package de.tub.dima.scotty.sparkconnector.demo;
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
