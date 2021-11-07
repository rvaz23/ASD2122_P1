package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class NotificationMessage extends ProtoMessage {
    public static final short MSG_ID = 103;

    private final UUID mid;
    private final Host sender;

    private final short toDeliver;


    public NotificationMessage(UUID mid, Host sender, short toDeliver) {
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

    public static ISerializer<NotificationMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(NotificationMessage notificationMessage, ByteBuf out) throws IOException {
            out.writeLong(notificationMessage.mid.getMostSignificantBits());
            out.writeLong(notificationMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(notificationMessage.sender, out);
            out.writeShort(notificationMessage.toDeliver);
        }


        @Override
        public NotificationMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();

            return new NotificationMessage(mid, sender, toDeliver);
        }
    };
}
