package de.tub.dima.scotty.slicing.state;

import de.tub.dima.scotty.core.windowFunction.*;
import de.tub.dima.scotty.slicing.slice.*;
import de.tub.dima.scotty.state.*;

public class AggregateValueState<Input, Partial, Output> {

    private final ValueState<Partial> partialAggregateState;
    private final AggregateFunction<Input, Partial, Output> aggregateFunction;
    private final SetState<StreamRecord<Input>> recordSetState;

    public AggregateValueState(ValueState<Partial> valueState, AggregateFunction<Input, Partial, Output> aggregateFunction, SetState<StreamRecord<Input>> recordSet) {
        this.partialAggregateState = valueState;
        this.aggregateFunction = aggregateFunction;
        this.recordSetState = recordSet;
    }

    /**
     * Add new element to a ValueState.
     * @param element
     */
    public void addElement(Input element) {
        if (partialAggregateState.isEmpty() || partialAggregateState.get() == null) {
            Partial liftedElement = aggregateFunction.lift(element);
            partialAggregateState.set(liftedElement);
        } else {
            Partial combined = aggregateFunction.liftAndCombine(partialAggregateState.get(), element);
            partialAggregateState.set(combined);
        }
    }

    public void removeElement(StreamRecord<Input> streamRecord){
        if(aggregateFunction instanceof InvertibleAggregateFunction){
            InvertibleAggregateFunction<Input,Partial,Output> invertibleAggregateFunction = (InvertibleAggregateFunction<Input,Partial,Output>) aggregateFunction;
            Partial newPartial = invertibleAggregateFunction.liftAndInvert(partialAggregateState.get(), streamRecord.record);
            partialAggregateState.set(newPartial);
        }else{
            recompute();
        }
    }

    public void recompute(){
        assert this.recordSetState != null;
        clear();
        for(StreamRecord<Input> streamRecord: this.recordSetState){
            addElement(streamRecord.record);
        }
    }

    public void clear(){
        partialAggregateState.clean();
    }

    public void merge(AggregateValueState<Input, Partial, Output> otherAggState) {
        ValueState<Partial> otherValueState = otherAggState.partialAggregateState;
        if (this.partialAggregateState.isEmpty() && !otherValueState.isEmpty()) {
            Partial otherValue = otherValueState.get();
            if (this.aggregateFunction instanceof CloneablePartialStateFunction) {
                otherValue = ((CloneablePartialStateFunction<Partial>) this.aggregateFunction).clone(otherValue);
            }
            this.partialAggregateState.set(otherValue);
        } else if (!otherValueState.isEmpty()) {
            Partial merged = this.aggregateFunction.combine(this.partialAggregateState.get(), otherValueState.get());
            this.partialAggregateState.set(merged);
        }


    }

    public Output getValue() {
        return this.aggregateFunction.lower(partialAggregateState.get());
    }

    @Override
    public String toString() {
        return aggregateFunction.getClass().getSimpleName() + "->" + this.partialAggregateState.toString();
    }
}
