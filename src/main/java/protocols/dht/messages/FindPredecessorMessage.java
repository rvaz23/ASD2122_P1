package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class FindPredecessorMessage extends ProtoMessage {

    public static final short MSG_ID = 101;

    private final UUID mid;
    private final Host sender;


    private final short toDeliver;


    public FindPredecessorMessage(UUID mid, Host sender, short toDeliver) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.toDeliver = toDeliver;

    }

    public UUID getMid() {
        return mid;
    }

    public Host getSender() {
        return sender;
    }

    public short getToDeliver() {
        return toDeliver;
    }


    public static ISerializer<FindPredecessorMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(FindPredecessorMessage findPredecessorMessage, ByteBuf out) throws IOException {
            out.writeLong(findPredecessorMessage.mid.getMostSignificantBits());
            out.writeLong(findPredecessorMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(findPredecessorMessage.sender, out);
            out.writeShort(findPredecessorMessage.toDeliver);

        }


        @Override
        public FindPredecessorMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();
            return new FindPredecessorMessage(mid, sender, toDeliver);
        }
    };
}
