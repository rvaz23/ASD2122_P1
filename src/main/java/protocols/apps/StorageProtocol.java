package protocols.apps;

import channel.notifications.ChannelCreated;
import channel.notifications.ConnectionDown;
import channel.notifications.ConnectionUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.dht.ConnectionEntry;
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

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.HashGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

public class StorageProtocol extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(StorageProtocol.class);

    public static final String PROTO_NAME = "StorageApp";
    public static final short PROTO_ID = 302;

    private static short dhtProtoId;
    private static short appProtoId;

    private Map<BigInteger, StorageEntry> pendingToStore;
    private Map<BigInteger, StorageEntry> storage;
    private final Set<Host> connections;
    private final HashMap<Host, Set<ProtoMessage>> pending;


    private final Host self;

    private boolean channelReady;

    public StorageProtocol(Host self, Properties properties, short appProtoId, short dhtProtoId) throws IOException, HandlerRegistrationException {
        super(PROTO_NAME, PROTO_ID);
        this.dhtProtoId = dhtProtoId;
        this.appProtoId = appProtoId;
        this.self = self;
        this.pendingToStore = new TreeMap<BigInteger, StorageEntry>();
        this.storage = new TreeMap<BigInteger, StorageEntry>();
        this.connections = new HashSet<Host>();
        this.pending = new HashMap<Host, Set<ProtoMessage>>();
        this.channelReady=false;


        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(StoreRequest.REQUEST_ID, this::uponStoreRequest);
        registerRequestHandler(RetrieveRequest.REQUEST_ID, this::uponRetrieveRequest);


        /*----------------------- Register Reply Handlers ----------------------------- */
        registerReplyHandler(LookupReply.REPLY_ID, this::uponLookUpReply);

        /*---------------------- Register Notification Handlers---------------------- */


        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(ConnectionUp.NOTIFICATION_ID, this::uponConnUp);
        subscribeNotification(ConnectionDown.NOTIFICATION_ID, this::uponConnDown);

    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {

        //triggerNotification(new ChannelCreated(channelId));

    }

    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
        int cId = notification.getChannelId();
        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);
        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, StoreMessage.MSG_ID, StoreMessage.serializer);
        registerMessageSerializer(cId, StoreMessageReply.MSG_ID, StoreMessageReply.serializer);
        registerMessageSerializer(cId, RetrieveMessage.MSG_ID, RetrieveMessage.serializer);
        registerMessageSerializer(cId, RetrieveResponseMessage.MSG_ID, RetrieveResponseMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        try {
            /*---------------------- Register Message Handlers -------------------------- */
            registerMessageHandler(cId, StoreMessage.MSG_ID, this::uponStoreMessage);
            registerMessageHandler(cId, StoreMessageReply.MSG_ID, this::uponStoreMessageReply);
            registerMessageHandler(cId, RetrieveMessage.MSG_ID, this::uponRetrieveMessage);
            registerMessageHandler(cId, RetrieveResponseMessage.MSG_ID, this::uponRetrieveResponseMessage);
        } catch (HandlerRegistrationException e) {
            logger.error("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        //Now we can start sending messages
        channelReady = true;
    }

    /*--------------------------------- Reply ---------------------------------------- */

    private void uponLookUpReply(LookupReply reply, short sourceProto) {
        logger.info("{}: LookUp response from content with peer: {} (replyID {})", self, reply.getPeer(), reply.getReplyUID());
        //SE LOOKUP FOR DE PENDINGTO STORE PEDIR PARA GUARDAR
        StorageEntry entry = pendingToStore.get(reply.getID());
        if (entry != null) {
            //Send Store Mesage for peer

            if (reply.getPeer().equals(self)) {
                storage.put(reply.getID(), entry);
                logger.info("{}: Store completed: {} ", self, entry.getName());
                StoreOKReply storeOKReply = new StoreOKReply(entry.getName(), UUID.randomUUID());
                sendReply(storeOKReply, appProtoId);
                pendingToStore.remove(reply.getID());

            } else {
                logger.info("{}: Store message to {} (replyID {})", self, reply.getPeer(), reply.getReplyUID());
                StoreMessage storeMessage = new StoreMessage(UUID.randomUUID(), self, sourceProto, reply.getID(), entry.getName(), entry.getContent());
                    trySendMessage(storeMessage, reply.getPeer());
            }

        } else {
            logger.info("{}: Retrieve {} (replyID {})", self, reply.getPeer(), reply.getReplyUID());
            //obter conteudo de peer send retrieve message
            //RetrieveMessage retrieveMessage = new RetrieveMessage(reply.getReplyUID(), self, PROTO_ID, reply.getID());
            //trySendMessage(retrieveMessage, reply.getPeer());

        }
    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponStoreRequest(StoreRequest request, short sourceProto) {
        logger.info("Store request from app");
        BigInteger idHash = HashGenerator.positiveBig(HashGenerator.generateHash(request.getName()));
        StorageEntry storageEntry = new StorageEntry(request.getName(), request.getContent());
        pendingToStore.put(idHash, storageEntry);
        LookupRequest lookupRequest = new LookupRequest(idHash, UUID.randomUUID());
        sendRequest(lookupRequest, dhtProtoId);

    }

    private void uponRetrieveRequest(RetrieveRequest request, short sourceProto) {
        BigInteger id = HashGenerator.positiveBig(HashGenerator.generateHash(request.getName()));
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
        logger.info("Recebeu store de {}, self: : ",msg.getSender(),self);
        if (!channelReady) return;
        StorageEntry storageEntry = new StorageEntry(msg.getContentName(), msg.getContent());
        storage.put(msg.getHash(), storageEntry);
        if(!self.equals(msg.getSender())){
            StoreMessageReply storeMessageReply = new StoreMessageReply(UUID.randomUUID(), self, getProtoId(), msg.getHash());
            trySendMessage(storeMessageReply, msg.getSender());
        }else{
            StoreOKReply ok = new StoreOKReply(storageEntry.getName(), UUID.randomUUID());
            sendReply(ok,appProtoId);
        }

    }

    private void uponStoreMessageReply(StoreMessageReply reply, Host from, short sourceProto, int channelId) {
        //Wait for storeOKMessage response
        StorageEntry entry = pendingToStore.get(reply.getHash());
        if (entry != null) {
            logger.info("{}: Store completed: {} ", self, entry.getName());
            StoreOKReply storeOKReply = new StoreOKReply(entry.getName(), UUID.randomUUID());
            sendReply(storeOKReply, appProtoId);
            pendingToStore.remove(reply.getHash());
        }
    }

    private void uponRetrieveMessage(RetrieveMessage msg, Host from, short sourceProto, int channelId) {
        //Wait for storeOKMessage response
        if (!channelReady) return;
        StorageEntry storageEntry = storage.get(msg.getCid());
        if (storageEntry != null) {
            RetrieveResponseMessage retrieveResponseMessage = new RetrieveResponseMessage(UUID.randomUUID(), self, getProtoId(), msg.getCid(), storageEntry.getName(), storageEntry.getContent());
            trySendMessage(retrieveResponseMessage, msg.getSender());
        } else {
            //TODO retornar msg conteudo vazio???
        }
    }

    private void uponRetrieveResponseMessage(RetrieveResponseMessage reply, Host from, short sourceProto, int channelId) {
        //Wait for storeOKMessage response
        RetrieveOKReply retrieveOk = new RetrieveOKReply(reply.getName(), UUID.randomUUID(), reply.getContent());
        sendReply(retrieveOk, sourceProto);
    }

    //------------------- Connection ---------------
    private void uponConnUp(ConnectionUp notification, short sourceProto) {
        for(Host h: notification.getNeighbours()) {
            connections.add(h);
            logger.info("New connection: " + h);

            Set<ProtoMessage> pendingMessages = pending.get(h);
            if (pendingMessages != null && !pendingMessages.isEmpty()) {
                for (ProtoMessage protoMessage : pendingMessages)
                    trySendMessage(protoMessage, h);
                pendingMessages.remove(h);
            }

        }
    }

    private void uponConnDown(ConnectionDown notification, short sourceProto) {
        for(Host h: notification.getNeighbours()) {
            connections.remove(h);
            logger.info("Connection down: " + h);
        }
    }

    private void trySendMessage(ProtoMessage message, Host destination) {
        if (connections.contains(destination)){
            //there's an open connection to the destination
            logger.info("Envia para : " + destination);
        sendMessage(message, destination);

    }else {
            //there's no open connection to the destination. need to open one
            Set<ProtoMessage> pendingMessages = pending.get(destination);
            if (pendingMessages == null)
                pendingMessages = new HashSet<>();
            pendingMessages.add(message);
            pending.put(destination, pendingMessages);
            //openConnection(destination);
        }
    }

}
