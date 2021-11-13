package protocols.dht.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class CheckPredecessorTimer extends ProtoTimer {
    public static final short TIMER_ID = 101;

    public CheckPredecessorTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
