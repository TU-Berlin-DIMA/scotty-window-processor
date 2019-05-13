package de.tub.dima.scotty.slicing.slice;


import de.tub.dima.scotty.slicing.*;
import de.tub.dima.scotty.slicing.state.*;
import de.tub.dima.scotty.state.*;
import org.jetbrains.annotations.*;

import java.util.*;

public class LazySlice<InputType, ValueType> extends AbstractSlice<InputType, ValueType> {

    private final AggregateState<InputType> state;
    private final SetState<StreamRecord<InputType>> records;

    public LazySlice(StateFactory stateFactory, WindowManager windowManager, long startTs, long endTs, long startC, long endC, Type type) {
        super(startTs, endTs, startC, endC, type);
        this.records = stateFactory.createSetState();
        this.state = new AggregateState<>(stateFactory, windowManager.getAggregations(), this.records);
    }

    @Override
    public void addElement(InputType element, long ts) {
        super.addElement(element, ts);
        state.addElement(element);
        records.add(new StreamRecord(ts, element));
    }

    public void prependElement(StreamRecord<InputType> newElement) {
        super.addElement(newElement.record, newElement.ts);
        records.add(newElement);
        state.addElement(newElement.record);
    }

    public StreamRecord<InputType> dropLastElement() {
        StreamRecord<InputType> dropRecord = records.dropLast();
        StreamRecord<InputType> currentLast = records.getLast();
        this.setCLast(this.getCLast()-1);
        this.setTLast(currentLast.ts);
        this.state.removeElement(dropRecord);
        return dropRecord;
    }

    @Override
    public AggregateState getAggState() {
        return state;
    }


}
