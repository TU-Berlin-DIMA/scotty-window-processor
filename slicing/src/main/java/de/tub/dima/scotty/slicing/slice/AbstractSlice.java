package de.tub.dima.scotty.slicing.slice;

public abstract class AbstractSlice<InputType, ValueType> implements Slice<InputType, ValueType> {

    private long tStart;
    private long tEnd;

    private Type type;

    private long tLast;
    private long tFirst;

    public AbstractSlice(long startTs, long endTs, Type type) {
        this.type = type;
        this.tStart = startTs;
        this.tEnd = endTs;
    }


    @Override
    public void addElement(InputType element, long ts) {
        this.tLast = Math.max(this.tLast, ts);
        this.tFirst = Math.min(this.tFirst, ts);
    }

    @Override
    public void merge(Slice otherSlice) {
        this.tLast = Math.max(this.tLast, otherSlice.getTLast());
        this.tFirst = Math.min(this.tFirst, otherSlice.getTFirst());
        this.setTEnd(Math.max(this.tEnd, otherSlice.getTEnd()));
        this.getAggState().merge(otherSlice.getAggState());
    }



    @Override
    public long getTLast() {
        return tLast;
    }

    @Override
    public long getTFirst() {
        return this.tFirst;
    }

    @Override
    public long getTStart() {
        return tStart;
    }

    @Override
    public void setTStart(long tStart) {
        this.tStart = tStart;
    }

    @Override
    public long getTEnd() {
        return tEnd;
    }

    @Override
    public void setTEnd(long tEnd) {
        this.tEnd = tEnd;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public void setType(Type type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "Slice{" +
                "tStart=" + tStart +
                ", tEnd=" + tEnd +
                ", measure=" + type +
                ", tLast=" + tLast +
                ", tFirst=" + tFirst +
                '}';
    }
}
