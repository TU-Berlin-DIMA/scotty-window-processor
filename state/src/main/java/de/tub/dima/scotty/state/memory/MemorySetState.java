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