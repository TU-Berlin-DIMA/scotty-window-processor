package de.tub.dima.scotty.slicing.aggregationstore.test;

import de.tub.dima.scotty.state.*;
import de.tub.dima.scotty.state.memory.*;

public class StateFactoryMock implements StateFactory {
    @Override
    public <T> ValueState<T> createValueState() {
        return new ValueState<T>() {
            T value;

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

            }

            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public String toString() {
                return "" + value;
            }
        };
    }

    @Override
    public <T> ListState<T> createListState() {
        return null;
    }

    @Override
    public <T extends Comparable<T>> SetState<T> createSetState() {
        return new MemorySetState<>();
    }
}
