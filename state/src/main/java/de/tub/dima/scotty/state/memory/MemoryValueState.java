package de.tub.dima.scotty.state.memory;

import de.tub.dima.scotty.state.ValueState;

import java.util.*;

public class MemoryValueState<T> implements ValueState<T> {

    private T value;

    @Override
    public T get() {
        return value;
    }

    @Override
    public void set(T value) {
        this.value = value;
    }

    @Override
    public void clean() {
        value = null;
    }

    @Override
    public boolean isEmpty() {
        return this.value == null;
    }

    @Override
    public String toString() {
        if(isEmpty())
            return "";
        return value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemoryValueState<?> that = (MemoryValueState<?>) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
