package protocols.dht.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class NotificationMessage extends ProtoMessage {
    public static final short MSG_ID = 103;

    public NotificationMessage(UUID mid, Host sender, Host ofNode, short toDeliver) {
        super(MSG_ID);
    }
}
