package brisk.optimization.model;

import java.io.Serializable;

import static util.Constants.default_sourceRate;

/**
 * Created by tony on 7/4/2017.
 */
public class Variables implements Serializable {
    private static final long serialVersionUID = 40L;
    public double SOURCE_RATE = default_sourceRate / 1.0E+09;//event/ns

    public Variables(Variables variables) {
        this.SOURCE_RATE = variables.SOURCE_RATE;
    }

    public Variables() {

    }
}
