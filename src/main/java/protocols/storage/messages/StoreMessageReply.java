package protocols.storage.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class StoreMessageReply extends ProtoMessage {
    public static final short MSG_ID = 204;

    private final UUID mid;
    private final Host sender;

    private final short toDeliver;
    private final BigInteger hash;




    @Override
    public String toString() {
        return "RetrieveMessage{" +
                "mid=" + mid +
                '}';
    }

    public StoreMessageReply(UUID mid, Host sender, short toDeliver, BigInteger hash) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.toDeliver = toDeliver;
        this.hash = hash;
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

    //ATENTION BIGINT 8 BYTES
    public static ISerializer<StoreMessageReply> serializer = new ISerializer<>() {
        @Override
        public void serialize(StoreMessageReply storeMessageReply, ByteBuf out) throws IOException {
            out.writeLong(storeMessageReply.mid.getMostSignificantBits());
            out.writeLong(storeMessageReply.mid.getLeastSignificantBits());
            Host.serializer.serialize(storeMessageReply.sender, out);
            out.writeShort(storeMessageReply.toDeliver);
            out.writeBytes(storeMessageReply.hash.toByteArray());

        }


        @Override
        public StoreMessageReply deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();
            byte[] big = in.readBytes(20).array();
            BigInteger hash = new BigInteger(big);
            return new StoreMessageReply(mid, sender,toDeliver, hash);
        }
    };
}
