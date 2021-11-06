package protocols.storage.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class RetrieveResponseMessage extends ProtoMessage {


    public static final short MSG_ID = 202;

    private final UUID mid;
    private final Host sender;

    private final short toDeliver;
    private final BigInteger cid;

    private final String name;

    private final byte[] content;

    public RetrieveResponseMessage(UUID mid, Host sender, short toDeliver, BigInteger cid, String name, byte[] content) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.toDeliver=toDeliver;
        this.cid = cid;
        this.name = name;
        this.content = content;
    }

    @Override
    public String toString() {
        return "RetrieveResponseMessage{" +
                "mid=" + mid +
                '}';
    }


    public Host getSender() {
        return sender;
    }

    public UUID getMid() {
        return mid;
    }

    public short getToDeliver() {
        return toDeliver;
    }

    public BigInteger getCid() {
        return cid;
    }

    public String getName() {
        return name;
    }

    public byte[] getContent() {
        return content;
    }

    public static ISerializer<RetrieveResponseMessage> getSerializer() {
        return serializer;
    }

    public static ISerializer<RetrieveResponseMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(RetrieveResponseMessage retrieveMessage, ByteBuf out) throws IOException {
            out.writeLong(retrieveMessage.mid.getMostSignificantBits());
            out.writeLong(retrieveMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(retrieveMessage.sender, out);
            out.writeShort(retrieveMessage.toDeliver);
            out.writeBytes(retrieveMessage.cid.toByteArray());
            out.writeInt(retrieveMessage.name.length());
            if (retrieveMessage.name.length() > 0) {
                out.writeBytes(retrieveMessage.name.getBytes(StandardCharsets.UTF_8));
            }
            out.writeInt(retrieveMessage.content.length);
            if (retrieveMessage.content.length > 0) {
                out.writeBytes(retrieveMessage.content);
            }
        }


        @Override
        public RetrieveResponseMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();
            byte[] big = in.readBytes(8).array();
            BigInteger cid = new BigInteger(big);
            int size = in.readInt();
            byte[] nameByte = new byte[size];
            if (size > 0)
                in.readBytes(nameByte);
             size = in.readInt();
            byte[] content = new byte[size];
            if (size > 0)
                in.readBytes(content);

            return new RetrieveResponseMessage(mid, sender, toDeliver ,cid, new String(nameByte,StandardCharsets.UTF_8),content);
        }
    };


}
