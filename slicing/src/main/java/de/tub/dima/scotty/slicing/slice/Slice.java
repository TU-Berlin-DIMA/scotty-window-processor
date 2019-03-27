package de.tub.dima.scotty.slicing.slice;

import de.tub.dima.scotty.slicing.state.*;

public interface Slice<InputType, ValueType> {

    /**
     * @return slice start timestamp
     */
    long getTStart();

    void setTStart(long tStart);

    /**
     * @return slice end timestamp
     */
    long getTEnd();

    void setTEnd(long tEnd);

    /**
     * timestamp of the first record in the slice
     *
     * @return long
     */
    long getTFirst();

    void merge(Slice otherSlice);

    /**
     * timestamp of the last record in the slice
     *
     * @return long
     */
    long getTLast();


    /**
     * The measure of the slice end.
     *
     * @return Type
     */
    Type getType();

    /**
     * Set the end of the slice
     *
     * @param type of slice end
     */
    void setType(Type type);

    /**
     * @return
     */
    AggregateState getAggState();

    /**
     * Add a element to the slice.
     *
     * @param element the element which is added
     * @param ts      timestamp of the element
     */
    void addElement(InputType element, long ts);

    default void removeElement(InputType element){

    };

    /**
     * Element count of first element
     * @return
     */
    long getCStart();

    /**
     * Element count of last element
     * @return
     */
    long getCLast();


    interface Type {
        boolean isMovable();
    }

    public final class Fixed implements Type {

        @Override
        public boolean isMovable() {
            return false;
        }
    }

    public final class Flexible implements Type {
        private int counter;

        public Flexible() {
            this(1);
        }

        public Flexible(int counter) {
            this.counter = counter;
        }

        public long getCount() {
            return counter;
        }

        public void decrementCount() {
            counter--;
        }

        public void incrementCount() {
            counter++;
        }

        @Override
        public boolean isMovable() {
            return getCount() == 1;
        }
    }
}