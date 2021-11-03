package protocols.dht.replies;

import protocols.storage.replies.RetrieveFailedReply;
import pt.unl.fct.di.novasys.babel.generic.ProtoReply;

import java.math.BigInteger;
import java.util.UUID;

public class LookupReply extends ProtoReply {

    final public static short REPLY_ID = 301;

    private BigInteger id;
    private UUID uid;
    private String peer;

    public LookupReply(BigInteger id, String peer, UUID uid) {
        super(LookupReply.REPLY_ID);
        this.id = id;
        this.peer = peer;
        this.uid = uid;
    }

    public UUID getReplyUID() {
        return this.uid;
    }

    public BigInteger getID() {
        return this.id;
    }

    public String getPeer() {
        return this.peer;
    }

}
