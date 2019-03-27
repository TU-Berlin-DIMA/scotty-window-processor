package de.tub.dima.scotty.state;

import java.io.*;

public interface StateFactory extends Serializable {

    <T> ValueState<T> createValueState();

    <T> ListState<T> createListState();

    <T extends Comparable<T>> SetState<T> createSetState();
}
