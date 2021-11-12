package protocols.storage.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.UUID;


public class StoreMessage extends ProtoMessage {

    public static final short MSG_ID = 201;

    private final UUID mid;
    private final Host sender;

    private final short toDeliver;
    private final BigInteger hash;
    private final String contentName;
    private final byte[] content;




    @Override
    public String toString() {
        return "RetrieveMessage{" +
                "mid=" + mid +
                '}';
    }

    public StoreMessage(UUID mid, Host sender, short toDeliver, BigInteger hash, String contentName, byte[] content) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.toDeliver = toDeliver;
        this.hash = hash;
        this.contentName=contentName;
        this.content=content;
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

    public BigInteger getHash() {
        return hash;
    }

    public String getContentName() {
        return contentName;
    }

    public byte[] getContent() {
        return content;
    }

    public static ISerializer<StoreMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(StoreMessage storeMessage, ByteBuf out) throws IOException {
            out.writeLong(storeMessage.mid.getMostSignificantBits());
            out.writeLong(storeMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(storeMessage.sender, out);
            out.writeShort(storeMessage.toDeliver);
            out.writeBytes(storeMessage.hash.toByteArray());
            out.writeInt(storeMessage.contentName.length());
            if (storeMessage.contentName.length() > 0) {
                out.writeBytes(storeMessage.contentName.getBytes(StandardCharsets.UTF_8));
            }
            out.writeInt(storeMessage.content.length);
            if (storeMessage.content.length > 0) {
                out.writeBytes(storeMessage.content);
            }
        }


        @Override
        public StoreMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();
            byte[] big = in.readBytes(20).array();
            BigInteger hash = new BigInteger(big);

            int size = in.readInt();
            byte[] nameByte = new byte[size];
            if (size > 0)
                in.readBytes(nameByte);
            String contentName = new String(nameByte,StandardCharsets.UTF_8);
            size = in.readInt();
            byte[] content = new byte[size];
            if (size > 0)
                in.readBytes(content);

            return new StoreMessage(mid, sender,toDeliver, hash,contentName,content);
        }
    };
}
