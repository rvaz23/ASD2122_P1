package protocols.apps;

import channel.notifications.ChannelCreated;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.replies.LookupReply;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.Properties;

public class StorageProtocol extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(StorageProtocol.class);

    public static final String PROTO_NAME = "StorageApp";
    public static final short PROTO_ID = 301;

    private static short storageProtoId;
    private final int channelId;

    private final Host self;

    public StorageProtocol(Host self, Properties properties, short storageProtoId) throws IOException, HandlerRegistrationException {
        super(PROTO_NAME, PROTO_ID);
        this.storageProtoId = storageProtoId;
        this.self = self;

        Properties channelProps = new Properties();
        channelId = createChannel(TCPChannel.NAME, channelProps);

        registerReplyHandler(LookupReply.REPLY_ID, this::uponLookUpResponse);

        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);

    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {

        triggerNotification(new ChannelCreated(channelId));

    }

    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {

    }

    private void uponLookUpResponse(LookupReply reply, short sourceProto) {
        logger.info("{}: LookUp response from content with peer: {} (replyID {})", self, reply.getPeer(), reply.getReplyUID());
    }


}
