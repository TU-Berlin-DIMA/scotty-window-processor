package stream.scotty.core.windowType.windowContext;

public class DeleteModification implements WindowModifications {
    public final long pre;

    public DeleteModification(long pre) {
        this.pre = pre;
    }
}
