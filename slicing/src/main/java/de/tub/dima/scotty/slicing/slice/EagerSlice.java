package de.tub.dima.scotty.slicing.slice;


import de.tub.dima.scotty.slicing.AggregationState;
import de.tub.dima.scotty.slicing.WindowManager;
import de.tub.dima.scotty.state.StateFactory;

public class EagerSlice<InputType, ValueType> extends AbstractSlice<InputType, ValueType> {

    private final AggregationState<InputType> state;
    private final WindowManager windowManager;

    public EagerSlice(StateFactory stateFactory, WindowManager windowManager, long startTs, long endTs, Type type) {
        super(startTs, endTs, type);
        this.state = new AggregationState<InputType>(stateFactory, windowManager.getAggregations());
        this.windowManager = windowManager;
    }

    @Override
    public AggregationState getAggState() {
        return state;
    }

    @Override
    public void addElement(InputType element, long ts) {
        super.addElement(element, ts);
        state.addElement(element);
    }


}
