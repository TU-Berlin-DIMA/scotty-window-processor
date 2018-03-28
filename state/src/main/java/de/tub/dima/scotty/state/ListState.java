package de.tub.dima.scotty.state;

import java.util.List;

public interface ListState<ItemType> extends State {

    List<ItemType> get();

    void set(final int i, final ItemType value);

    void add(final int i, final ItemType value);

}
