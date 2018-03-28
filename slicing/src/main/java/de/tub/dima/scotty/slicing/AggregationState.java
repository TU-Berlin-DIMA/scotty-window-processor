package de.tub.dima.scotty.slicing;

import de.tub.dima.scotty.state.StateFactory;
import de.tub.dima.scotty.state.ValueState;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class AggregationState<InputType> implements Serializable {
    private final List<AggregateFunction> windowFunctions;
    private final List<ValueState<Object>> valueStates;

    private boolean empty;

    public AggregationState(StateFactory stateFactory, List<AggregateFunction> windowFunctions) {
        this.windowFunctions = windowFunctions;
        this.valueStates = new ArrayList<>();
        for (int i = 0; i < windowFunctions.size(); i++) {
            this.valueStates.add(stateFactory.createValueState());
        }
    }

    public void addElement(InputType state) {
        for (int i = 0; i < windowFunctions.size(); i++) {
            addElementToValueState(state, valueStates.get(i), windowFunctions.get(i));
        }
    }

    public void addElementToValueState(InputType element, ValueState<Object> valueState, AggregateFunction windowFunction){
        Object liftedElement = windowFunction.lift(element);
        if(valueState.isEmpty()  || valueState.get() == null){
            valueState.set(liftedElement);
        }else{
            Object combined = windowFunction.combine(valueState.get(), liftedElement);
            valueState.set(combined);
        }
    }

    public void merge(AggregationState<InputType> otherAggState) {
        if (this.isMergeable(otherAggState)) {
            for (int i = 0; i < otherAggState.valueStates.size(); i++) {
                AggregateFunction windowFunction = windowFunctions.get(i);
                if (this.valueStates.get(i).isEmpty())
                    this.valueStates.get(i).set(otherAggState.valueStates.get(i).get());
                else if(!otherAggState.valueStates.get(i).isEmpty()){
                    Object merged = windowFunction.combine(otherAggState.valueStates.get(i).get(), this.valueStates.get(i).get());
                    this.valueStates.get(i).set(merged);
                }
            }
        }
    }

    private boolean isMergeable(AggregationState otherAggState) {
        return otherAggState.windowFunctions.size() <= this.windowFunctions.size();
    }

    public List<Object> getValues() {
        List<Object> objectList = new ArrayList<>(valueStates.size());
        for (int i = 0; i < valueStates.size(); i++) {
            Object outputObject = windowFunctions.get(i).lower(valueStates.get(i).get());
            objectList.add(i, outputObject);
        }
        return objectList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregationState<?> that = (AggregationState<?>) o;
        return Objects.equals(windowFunctions, that.windowFunctions) &&
                Objects.equals(valueStates, that.valueStates);
    }

    @Override
    public int hashCode() {

        return Objects.hash(windowFunctions, valueStates);
    }

    @Override
    public String toString() {
        return "AggregationState{" +
                "windowFunctions=" + windowFunctions +
                ", valueStates=" + valueStates +
                '}';
    }
}
