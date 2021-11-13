package protocols.dht.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class FixFingerTimer extends ProtoTimer {

    public static final short TIMER_ID = 102;

    public FixFingerTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
