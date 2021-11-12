package protocols.apps;

import channel.notifications.ChannelCreated;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.dht.messages.*;
import protocols.dht.replies.LookupReply;
import protocols.dht.requests.LookupRequest;
import protocols.storage.StorageEntry;
import protocols.storage.messages.RetrieveMessage;
import protocols.storage.messages.RetrieveResponseMessage;
import protocols.storage.messages.StoreMessage;
import protocols.storage.messages.StoreMessageReply;
import protocols.storage.replies.RetrieveOKReply;
import protocols.storage.replies.StoreOKReply;
import protocols.storage.requests.RetrieveRequest;
import protocols.storage.requests.StoreRequest;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoReply;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.HashGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;

public class StorageProtocol extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(StorageProtocol.class);

    public static final String PROTO_NAME = "StorageApp";
    public static final short PROTO_ID = 302;

    private static short dhtProtoId;
    private static short appProtoId;
    private final int channelId;

    private Map<BigInteger, StorageEntry> pendingToStore;
    private Map<BigInteger, StorageEntry> storage;


    private final Host self;

    public StorageProtocol(Host self, Properties properties, short appProtoId, short dhtProtoId) throws IOException, HandlerRegistrationException {
        super(PROTO_NAME, PROTO_ID);
        this.dhtProtoId = dhtProtoId;
        this.appProtoId = appProtoId;
        this.self = self;
        this.pendingToStore = new TreeMap<BigInteger, StorageEntry>();

        Properties channelProps = new Properties();
        channelId = createChannel(TCPChannel.NAME, channelProps);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, StoreMessage.MSG_ID, StoreMessage.serializer);
        registerMessageSerializer(channelId, StoreMessageReply.MSG_ID, StoreMessageReply.serializer);
        registerMessageSerializer(channelId, RetrieveMessage.MSG_ID, RetrieveMessage.serializer);
        registerMessageSerializer(channelId, RetrieveResponseMessage.MSG_ID, RetrieveResponseMessage.serializer);


        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, StoreMessage.MSG_ID, this::uponStoreMessage);
        registerMessageHandler(channelId, StoreMessageReply.MSG_ID, this::uponStoreMessageReply);
        registerMessageHandler(channelId, RetrieveMessage.MSG_ID, this::uponRetrieveMessage);
        registerMessageHandler(channelId, RetrieveResponseMessage.MSG_ID, this::uponRetrieveResponseMessage);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(StoreRequest.REQUEST_ID, this::uponStoreRequest);
        registerRequestHandler(RetrieveRequest.REQUEST_ID, this::uponRetrieveRequest);


        /*----------------------- Register Reply Handlers ----------------------------- */
        registerReplyHandler(LookupReply.REPLY_ID, this::uponLookUpReply);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, RetrieveMessage.MSG_ID, RetrieveMessage.serializer);
        registerMessageSerializer(channelId, RetrieveResponseMessage.MSG_ID, RetrieveResponseMessage.serializer);

        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);

    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {

        triggerNotification(new ChannelCreated(channelId));

    }

    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {

    }

    /*--------------------------------- Reply ---------------------------------------- */

    private void uponLookUpReply(LookupReply reply, short sourceProto) {
        logger.info("{}: LookUp response from content with peer: {} (replyID {})", self, reply.getPeer(), reply.getReplyUID());
        //SE LOOKUP FOR DE PENDINGTO STORE PEDIR PARA GUARDAR
        StorageEntry entry = pendingToStore.get(reply.getID());
        openConnection(reply.getPeer());
        if (entry != null) {
            //Send Store Mesage for peer

            if (reply.getPeer().equals(self)) {
                storage.put(reply.getID(), entry);
                logger.info("{}: Store completed: {} ", self, entry.getName());
                StoreOKReply storeOKReply = new StoreOKReply(entry.getName(), UUID.randomUUID());
                sendReply(storeOKReply, sourceProto);
                pendingToStore.remove(entry);

            } else {
                StoreMessage storeMessage = new StoreMessage(UUID.randomUUID(), self, sourceProto, reply.getID(), entry.getName(), entry.getContent());
                sendMessage(storeMessage, reply.getPeer());
            }

        } else {
            //obter conteudo de peer send retrieve message
            RetrieveMessage retrieveMessage = new RetrieveMessage(reply.getReplyUID(), self, PROTO_ID, reply.getID());
            sendMessage(retrieveMessage, reply.getPeer());

        }
    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponStoreRequest(StoreRequest request, short sourceProto) {
        BigInteger idHash = HashGenerator.generateHash(request.getName());
        StorageEntry storageEntry = new StorageEntry(request.getName(), request.getContent());
        pendingToStore.put(idHash, storageEntry);
        LookupRequest lookupRequest = new LookupRequest(idHash, UUID.randomUUID());
        sendRequest(lookupRequest, dhtProtoId);

    }

    private void uponRetrieveRequest(RetrieveRequest request, short sourceProto) {
        BigInteger id = HashGenerator.generateHash(request.getName());
        if (storage.containsKey(id)) {
            StorageEntry storageEntry = storage.get(id);
            RetrieveOKReply retrieveOk = new RetrieveOKReply(storageEntry.getName(), request.getRequestUID(), storageEntry.getContent());
            sendReply(retrieveOk, sourceProto);
        } else {
            LookupRequest lookupRequest = new LookupRequest(id, request.getRequestUID());
            sendRequest(lookupRequest, dhtProtoId);
        }

    }

    /*--------------------------------- Messages ---------------------------------------- */

    private void uponStoreMessage(StoreMessage msg, Host from, short sourceProto, int channelId) {
        StorageEntry storageEntry = new StorageEntry(msg.getContentName(), msg.getContent());
        storage.put(msg.getHash(), storageEntry);
        openConnection(msg.getSender());
        StoreMessageReply storeMessageReply = new StoreMessageReply(UUID.randomUUID(), self, sourceProto, msg.getHash());
        sendMessage(storeMessageReply, msg.getSender());
        closeConnection(msg.getSender());
    }

    private void uponStoreMessageReply(StoreMessageReply reply, Host from, short sourceProto, int channelId) {
        //Wait for storeOKMessage response
        StorageEntry entry = pendingToStore.get(reply.getHash());
        if (entry != null) {
            logger.info("{}: Store completed: {} ", self, entry.getName());
            StoreOKReply storeOKReply = new StoreOKReply(entry.getName(), UUID.randomUUID());
            sendReply(storeOKReply, sourceProto);
            pendingToStore.remove(entry);
        }
    }

    private void uponRetrieveMessage(RetrieveMessage msg, Host from, short sourceProto, int channelId) {
        //Wait for storeOKMessage response
        StorageEntry storageEntry = storage.get(msg.getCid());
        if (storageEntry != null) {
            RetrieveResponseMessage retrieveResponseMessage = new RetrieveResponseMessage(UUID.randomUUID(), self, sourceProto, msg.getCid(), storageEntry.getName(), storageEntry.getContent());
            openConnection(msg.getSender());
            sendMessage(retrieveResponseMessage, msg.getSender());
            closeConnection(msg.getSender());
        } else {
            //TODO retornar msg conteudo vazio???
        }
    }

    private void uponRetrieveResponseMessage(RetrieveResponseMessage reply, Host from, short sourceProto, int channelId) {
        //Wait for storeOKMessage response
        RetrieveOKReply retrieveOk = new RetrieveOKReply(reply.getName(), UUID.randomUUID(), reply.getContent());
        sendReply(retrieveOk, sourceProto);
    }

}
