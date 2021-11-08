package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class LookUpReplyMessage extends ProtoMessage {

    public static final short MSG_ID = 105;

    private final UUID mid;
    private final Host sender;
    private final Host contentOwner;
    private final BigInteger contentHash;

    private final short toDeliver;

    public LookUpReplyMessage(UUID mid, Host sender, Host contentOwner,BigInteger contentHash, short toDeliver) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.contentOwner = contentOwner;
        this.toDeliver = toDeliver;
        this.contentHash=contentHash;
    }

    public UUID getMid() {
        return mid;
    }

    public Host getSender() {
        return sender;
    }

    public Host getContentOwner() {
        return contentOwner;
    }

    public BigInteger getContentHash() {
        return contentHash;
    }

    public short getToDeliver() {
        return toDeliver;
    }

    public static ISerializer<LookUpReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(LookUpReplyMessage lookUpReplyMessage, ByteBuf out) throws IOException {
            out.writeLong(lookUpReplyMessage.mid.getMostSignificantBits());
            out.writeLong(lookUpReplyMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(lookUpReplyMessage.sender, out);
            Host.serializer.serialize(lookUpReplyMessage.contentOwner, out);
            out.writeBytes(lookUpReplyMessage.contentHash.toByteArray());
            out.writeShort(lookUpReplyMessage.toDeliver);
        }


        @Override
        public LookUpReplyMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            Host contentOwner = Host.serializer.deserialize(in);
            byte[] hashByteArray =new byte[20];
            for (int i =0;i<20;i++) {
                hashByteArray[i] = in.readByte();
            }
            BigInteger contentHash = new BigInteger(hashByteArray);
            short toDeliver = in.readShort();

            return new LookUpReplyMessage(mid, sender, contentOwner,contentHash ,toDeliver);
        }
    };
}
