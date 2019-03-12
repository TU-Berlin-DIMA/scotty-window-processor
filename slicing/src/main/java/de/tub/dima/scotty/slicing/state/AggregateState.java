package de.tub.dima.scotty.slicing.state;

import de.tub.dima.scotty.core.windowFunction.*;
import de.tub.dima.scotty.state.*;

import java.io.*;
import java.util.*;

public class AggregateState<InputType> implements Serializable {
    private final List<AggregateValueState<InputType, Object, Object>> aggregateValueStates;

    public AggregateState(StateFactory stateFactory, List<AggregateFunction> windowFunctions) {

        this.aggregateValueStates = new ArrayList<>();
        for (int i = 0; i < windowFunctions.size(); i++) {
            this.aggregateValueStates.add(new AggregateValueState<>(stateFactory.createValueState(), windowFunctions.get(i)));
        }
    }

    public void addElement(InputType state) {
        for (AggregateValueState<InputType, Object, Object> valueState : aggregateValueStates) {
            valueState.addElement(state);
        }
    }


    public void merge(AggregateState<InputType> otherAggState) {
        if (this.isMergeable(otherAggState)) {
            for (int i = 0; i < otherAggState.aggregateValueStates.size(); i++) {
                this.aggregateValueStates.get(i).merge(otherAggState.aggregateValueStates.get(i));
            }
        }
    }

    private boolean isMergeable(AggregateState otherAggState) {
        return otherAggState.aggregateValueStates.size() <= this.aggregateValueStates.size();
    }

    public List<Object> getValues() {
        List<Object> objectList = new ArrayList<>(aggregateValueStates.size());
        for (AggregateValueState<InputType, Object, Object> valueState : aggregateValueStates) {
            objectList.add(valueState.getValue());
        }
        return objectList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateState<?> that = (AggregateState<?>) o;
        return aggregateValueStates.equals(((AggregateState<?>) o).aggregateValueStates);
    }

    @Override
    public int hashCode() {

        return Objects.hash(aggregateValueStates);
    }

    //? To Be Changed
    @Override
    public String toString() {
        return "AggregateState{" +
                // "values=" + aggregateValueStates + '}';
                "values=" + printValues();
    }

    private String printValues() {
        String str = "";
        for (Object o : getValues())
            str += o.toString();
        return str;
    }
}
