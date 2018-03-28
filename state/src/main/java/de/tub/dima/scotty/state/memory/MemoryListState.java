package de.tub.dima.scotty.state.memory;

import de.tub.dima.scotty.state.ListState;

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
