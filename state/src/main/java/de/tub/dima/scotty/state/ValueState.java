package de.tub.dima.scotty.state;

public interface ValueState<ValueType> extends State {

    ValueType get();

    void set(final ValueType value);

}
