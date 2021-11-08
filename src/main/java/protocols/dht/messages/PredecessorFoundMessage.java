package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class PredecessorFoundMessage extends ProtoMessage {

    public static final short MSG_ID = 102;

    private final UUID mid;
    private final Host predecessor;

    private final short toDeliver;

    public PredecessorFoundMessage(UUID mid, Host predecessor, short toDeliver) {
        super(MSG_ID);
        this.mid = mid;
        this.predecessor = predecessor;
        this.toDeliver = toDeliver;
    }

    public UUID getMid() {
        return mid;
    }

    public Host getPredecessor() {
        return predecessor;
    }

    public short getToDeliver() {
        return toDeliver;
    }


    public static ISerializer<PredecessorFoundMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PredecessorFoundMessage predecessorFoundMessage, ByteBuf out) throws IOException {
            out.writeLong(predecessorFoundMessage.mid.getMostSignificantBits());
            out.writeLong(predecessorFoundMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(predecessorFoundMessage.predecessor, out);
            out.writeShort(predecessorFoundMessage.toDeliver);

            /*out.writeInt(floodMessage.content.length);
            if (floodMessage.content.length > 0) {
                out.writeBytes(floodMessage.content);
            }*/
        }


        @Override
        public PredecessorFoundMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host predecessor = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();

            return new PredecessorFoundMessage(mid, predecessor, toDeliver);
        }
    };
}
