package protocols.dht;

import pt.unl.fct.di.novasys.network.data.Host;

import java.util.Timer;

public class FingerEntry {

    public long lastTimeRead;

    public Host peer;

    public FingerEntry(long lastTimeRead, Host peer) {
        this.lastTimeRead = lastTimeRead;
        this.peer = peer;
    }

    public long getLastTimeRead() {
        return lastTimeRead;
    }

    public Host getPeer() {
        return peer;
    }
}
