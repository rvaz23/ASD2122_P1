package protocols.apps;

import channel.notifications.ChannelCreated;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.dht.replies.LookupReply;
import protocols.dht.requests.LookupRequest;
import protocols.storage.StorageEntry;
import protocols.storage.messages.RetrieveMessage;
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
    public static final short PROTO_ID = 301;

    private static short dhtProtoId;
    private static short appProtoId;
    private final int channelId;
    
    private Map<String, StorageEntry> storage;

    private final Host self;

    public StorageProtocol(Host self, Properties properties,short appProtoId, short dhtProtoId) throws IOException, HandlerRegistrationException {
        super(PROTO_NAME, PROTO_ID);
        this.dhtProtoId = dhtProtoId;
        this.appProtoId = appProtoId;
        this.self = self;
        this.storage= new  TreeMap<String,StorageEntry>();

        Properties channelProps = new Properties();
        channelId = createChannel(TCPChannel.NAME, channelProps);

        
        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(StoreRequest.REQUEST_ID, this::uponStoreRequest);
        registerRequestHandler(RetrieveRequest.REQUEST_ID, this::uponRetrieveRequest);
        
        
        /*----------------------- Register Reply Handlers ----------------------------- */
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
        //processar a informação e pedir ao peer o conteudo
        //CRIAR RETRIEVE MESSAGE
        RetrieveMessage retrieveMessage = new RetrieveMessage(reply.getReplyUID(),self,reply.getID());
        Host dest = new Host(peer);
        sendMessage(retrieveMessage,);
    }
    
    /*--------------------------------- Requests ---------------------------------------- */
    private void uponStoreRequest(StoreRequest request, short sourceProto) {
        StorageEntry storageEntry = new StorageEntry(request.getName(), request.getContent());
    	storage.put(request.getName(), storageEntry);
    	 logger.info("{}: Store completed: {} ", self, request.getRequestUID());
    	 StoreOKReply reply = new StoreOKReply(request.getName(), request.getRequestUID());
    	 sendReply(reply, sourceProto);

    }
    
    private void uponRetrieveRequest(RetrieveRequest request, short sourceProto) {
        BigInteger id= HashGenerator.generateHash(request.getName());
        if(storage.containsKey(id)){
            StorageEntry storageEntry =storage.get(id);
            RetrieveOKReply retrieveOk = new RetrieveOKReply(storageEntry.getName(), request.getRequestUID(),storageEntry.getContent());
            sendReply(retrieveOk,sourceProto);
        }else{
            LookupRequest lookupRequest = new LookupRequest(id,request.getRequestUID());
            sendRequest(lookupRequest,dhtProtoId);
        }

    }


}
