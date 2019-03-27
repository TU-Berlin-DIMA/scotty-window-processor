package de.tub.dima.scotty.state;

public interface SetState<Type extends Comparable<Type>> extends State, Iterable<Type> {

    public Type getLast();

    public Type getFirst();

    public Type dropLast();

    public Type dropFrist();

    public void add(Type record);

}
