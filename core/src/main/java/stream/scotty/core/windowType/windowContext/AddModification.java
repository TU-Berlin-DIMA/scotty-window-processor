package stream.scotty.core.windowType.windowContext;

public class AddModification implements WindowModifications {
    public final long post;

    public AddModification(long post) {
        this.post = post;
    }
}