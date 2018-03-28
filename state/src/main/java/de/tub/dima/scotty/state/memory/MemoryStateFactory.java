package de.tub.dima.scotty.state.memory;

import de.tub.dima.scotty.state.ListState;
import de.tub.dima.scotty.state.StateFactory;
import de.tub.dima.scotty.state.ValueState;

public class MemoryStateFactory implements StateFactory {
    @Override
    public <T> ValueState<T> createValueState() {
        return new MemoryValueState<>();
    }

    @Override
    public <T> ListState<T> createListState() {
        return new MemoryListState<>();
    }
}
