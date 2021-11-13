package protocols.apps.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class RequestTimer extends ProtoTimer {
    public static final short TIMER_ID = 304;

    public RequestTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
