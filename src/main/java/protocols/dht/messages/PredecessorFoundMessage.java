package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class PredecessorFoundMessage extends ProtoMessage {

    public static final short MSG_ID = 106;

    private final UUID mid;
    private final Host predecessor;
    private final Set<Host> successors;

    private final short toDeliver;

    public PredecessorFoundMessage(UUID mid, Host predecessor,Set<Host> successors, short toDeliver) {
        super(MSG_ID);
        this.mid = mid;
        this.predecessor = predecessor;
        this.toDeliver = toDeliver;
        this.successors=successors;
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

    public Set<Host> getSuccessors() {
        return successors;
    }

    public static ISerializer<PredecessorFoundMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PredecessorFoundMessage predecessorFoundMessage, ByteBuf out) throws IOException {
            out.writeLong(predecessorFoundMessage.mid.getMostSignificantBits());
            out.writeLong(predecessorFoundMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(predecessorFoundMessage.predecessor, out);
            out.writeShort(predecessorFoundMessage.toDeliver);
            out.writeInt(predecessorFoundMessage.successors.size());
            for (Host h : predecessorFoundMessage.getSuccessors()){
                Host.serializer.serialize(h, out);
            }
        }


        @Override
        public PredecessorFoundMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host predecessor = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();
            int size = in.readInt();
            Set<Host> successors = new HashSet<Host>();
            for (int i =0;i<size;i++){
                successors.add(Host.serializer.deserialize(in));
            }

            return new PredecessorFoundMessage(mid, predecessor, successors ,toDeliver);
        }
    };
}
