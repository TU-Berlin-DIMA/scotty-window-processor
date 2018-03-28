package de.tub.dima.scotty.core.windowType.windowContext;

public class ShiftModification implements WindowModifications {
    public final long pre;
    public final long post;

    public ShiftModification(long pre, long post) {
        this.pre = pre;
        this.post = post;
    }
}