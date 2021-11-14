package protocols.dht;

import pt.unl.fct.di.novasys.network.data.Host;


public class ConnectionEntry {

    public long lastTimeRead;

    public Host peer;

    public ConnectionEntry(long lastTimeRead, Host peer) {
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
