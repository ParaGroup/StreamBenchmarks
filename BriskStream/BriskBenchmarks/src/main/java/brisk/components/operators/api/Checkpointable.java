package brisk.components.operators.api;

import brisk.execution.runtime.tuple.impl.Marker;

public interface Checkpointable {

    void forward_checkpoint(int sourceId, long bid, Marker marker) throws InterruptedException;

    void forward_checkpoint(int sourceTask, String streamId, long bid, Marker marker) throws InterruptedException;

    /**
     * Optionally relax_reset state before marker.
     *
     * @param marker
     */
    void ack_checkpoint(Marker marker);


    void earlier_ack_checkpoint(Marker marker);
}
