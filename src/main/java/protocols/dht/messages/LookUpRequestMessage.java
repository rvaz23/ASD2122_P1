package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class LookUpRequestMessage extends ProtoMessage {

    public static final short MSG_ID = 104;

    private final UUID mid;
    private final Host sender;
    private final BigInteger contentHash;

    private final short toDeliver;

    public LookUpRequestMessage(UUID mid, Host sender, BigInteger contentHash, short toDeliver) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.contentHash = contentHash;
        this.toDeliver = toDeliver;
    }

    public UUID getMid() {
        return mid;
    }

    public Host getSender() {
        return sender;
    }

    public BigInteger getContentHash() {
        return contentHash;
    }

    public short getToDeliver() {
        return toDeliver;
    }

    public static ISerializer<LookUpRequestMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(LookUpRequestMessage lookUpRequestMessage, ByteBuf out) throws IOException {
            out.writeLong(lookUpRequestMessage.mid.getMostSignificantBits());
            out.writeLong(lookUpRequestMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(lookUpRequestMessage.sender, out);
            out.writeBytes(lookUpRequestMessage.contentHash.toByteArray());
            out.writeShort(lookUpRequestMessage.toDeliver);
        }


        @Override
        public LookUpRequestMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            byte[] hashByteArray =new byte[20];
            for (int i =0;i<20;i++) {
                hashByteArray[i] = in.readByte();
            }
            BigInteger contentHash = new BigInteger(hashByteArray);
            short toDeliver = in.readShort();

            return new LookUpRequestMessage(mid, sender, contentHash ,toDeliver);
        }
    };
}
