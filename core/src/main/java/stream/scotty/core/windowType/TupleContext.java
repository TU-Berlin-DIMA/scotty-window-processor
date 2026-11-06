package stream.scotty.core.windowType;

public interface TupleContext<Tuple> {

    Iterable<Tuple> iterator();
    Iterable<Tuple> iterator(int start, int end);
    Tuple lookup(int position);

}
