package de.tub.dima.scotty.slicing.state;

import de.tub.dima.scotty.core.windowFunction.*;
import de.tub.dima.scotty.state.*;

public class AggregateValueState<Input, Partial, Output> {

    private final ValueState<Partial> valueState;
    private final AggregateFunction<Input, Partial, Output> aggregateFunction;

    public AggregateValueState(ValueState<Partial> valueState, AggregateFunction<Input, Partial, Output> aggregateFunction) {
        this.valueState = valueState;
        this.aggregateFunction = aggregateFunction;
    }

    /**
     * Add new element to a ValueState.
     *
     * @param element
     */
    public void addElement(Input element) {
        if (valueState.isEmpty() || valueState.get() == null) {
            Partial liftedElement = aggregateFunction.lift(element);
            valueState.set(liftedElement);
        } else {
            Partial combined = aggregateFunction.liftAndCombine(valueState.get(), element);
            valueState.set(combined);
        }
    }

    public void merge(AggregateValueState<Input, Partial, Output> otherAggState) {
        ValueState<Partial> otherValueState = otherAggState.valueState;
        if (this.valueState.isEmpty() && !otherValueState.isEmpty()) {
            Partial otherValue = otherValueState.get();
            if (this.aggregateFunction instanceof CloneablePartialStateFunction) {
                otherValue = ((CloneablePartialStateFunction<Partial>) this.aggregateFunction).clone(otherValue);
            }
            this.valueState.set(otherValue);
        } else if (!otherValueState.isEmpty()) {
            Partial merged = this.aggregateFunction.combine(this.valueState.get(), otherValueState.get());
            this.valueState.set(merged);
        }


    }

    public Output getValue() {
        return this.aggregateFunction.lower(valueState.get());
    }
}
