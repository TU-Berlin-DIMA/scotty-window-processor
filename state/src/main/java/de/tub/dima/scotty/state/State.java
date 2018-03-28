package de.tub.dima.scotty.state;

import java.io.Serializable;

public interface State extends Serializable {

    void clean();

    boolean isEmpty();

}
