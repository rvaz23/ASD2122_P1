package protocols.apps.chord;

import channel.notifications.ChannelCreated;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.messages.*;
import protocols.dht.replies.LookupReply;
import protocols.dht.requests.LookupRequest;
import protocols.dht.timers.CheckPredecessorTimer;
import protocols.dht.timers.FixFingerTimer;
import protocols.dht.timers.StabilizeTimer;
import protocols.storage.StorageEntry;
import protocols.storage.replies.StoreOKReply;
import protocols.storage.requests.StoreRequest;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.HashGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.*;

public class ChordProtocol extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(ChordProtocol.class);

    public static final String PROTO_NAME = "ChordApplication";
    public static final short PROTO_ID = 301;
    private static final int SIZE = 5;


    private Host predecessor, successor;
    private BigInteger selfID;
    //private HashMap<Long, ChordProtocol> fingerTable;
    private boolean hasFailed;
    private short storageProtoId;
    private Host self;
    private final Set<Host> connectedTo; //Peers I am connected to
    private final Set<Host> connectedFrom; //Peers I am connected to
    private final Set<Host> pending; //Peers I am trying to connect to
    private HashMap<BigInteger, Host> fingerTable; //Peers that i know
    private HashMap<BigInteger, Host> contentTable;


    private final int sampleTime; //param: timeout for samples
    private final int subsetSize; //param: maximum size of sample;

    //Variables related with measurement
    private long storeRequests = 0;
    private long storeRequestsCompleted = 0;
    private long retrieveRequests = 0;
    private long retrieveRequestsSuccessful = 0;
    private long retrieveRequestsFailed = 0;

    private final int channelId; //Id of the created channel

    public ChordProtocol(Properties properties, Host self,short storageProtoId) throws IOException, HandlerRegistrationException {
        super(PROTO_NAME, PROTO_ID);
        this.self = self;
        this.storageProtoId=storageProtoId;
        this.connectedTo = new HashSet<>();
        this.connectedFrom = new HashSet<>();
        this.pending = new HashSet<>();
        this.selfID = HashGenerator.generateHash(self.toString());
        this.fingerTable = new HashMap<BigInteger, Host>();
        for (BigInteger finger : computeFingerNumbers(SIZE)) {
            fingerTable.put(new BigInteger(String.valueOf(finger)), null);
        }
        predecessor = null;
        successor = null;


        //Get some configurations from the Properties object
        this.subsetSize = Integer.parseInt(properties.getProperty("sample_size", "6"));
        this.sampleTime = Integer.parseInt(properties.getProperty("sample_time", "2000")); //2 seconds

        String cMetricsInterval = properties.getProperty("channel_metrics_interval", "10000"); //10 seconds

        //Create a properties object to setup channel-specific properties. See the channel description for more details.
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, properties.getProperty("address")); //The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, properties.getProperty("port")); //The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); //The interval to receive channel metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); //Heartbeats interval for established connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); //Time passed without heartbeats until closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); //TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); //Create the channel with the given properties

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(LookupRequest.REQUEST_ID, this::uponLookUpRequest);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, FindSuccessorMessage.MSG_ID, FindSuccessorMessage.serializer);
        registerMessageSerializer(channelId, SuccessorFoundMessage.MSG_ID, SuccessorFoundMessage.serializer);
        registerMessageSerializer(channelId, FindPredecessorMessage.MSG_ID, FindPredecessorMessage.serializer);
        registerMessageSerializer(channelId, PredecessorFoundMessage.MSG_ID, PredecessorFoundMessage.serializer);
        registerMessageSerializer(channelId, NotificationMessage.MSG_ID, NotificationMessage.serializer);
        registerMessageSerializer(channelId, LookUpRequestMessage.MSG_ID, LookUpRequestMessage.serializer);
        registerMessageSerializer(channelId, LookUpReplyMessage.MSG_ID, LookUpReplyMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, FindSuccessorMessage.MSG_ID, this::uponFindSuccessor);//, this::uponMsgFail);
        registerMessageHandler(channelId, SuccessorFoundMessage.MSG_ID, this::uponFoundSuccessor);
        registerMessageHandler(channelId, FindPredecessorMessage.MSG_ID, this::uponFindPredecessor);
        registerMessageHandler(channelId, PredecessorFoundMessage.MSG_ID, this::uponFoundPredecessor);
        registerMessageHandler(channelId, NotificationMessage.MSG_ID, this::uponNotify);
        registerMessageHandler(channelId, LookUpRequestMessage.MSG_ID, this::uponLookUpRequestMessage);
        registerMessageHandler(channelId, LookUpReplyMessage.MSG_ID, this::uponLookUpReplyMessage);

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(FixFingerTimer.TIMER_ID, this::uponFixFinger);
        registerTimerHandler(StabilizeTimer.TIMER_ID, this::uponStabilize);
        registerTimerHandler(CheckPredecessorTimer.TIMER_ID, this::uponCheckPredecessor);
        //registerTimerHandler(InfoTimer.TIMER_ID, this::uponInfoTime);

        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);//avisar o sucessor do sucessor que falhou
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);//Fazer join ou notify?
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        //registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {
        //Inform the dissemination protocol about the channel we created in the constructor
        triggerNotification(new ChannelCreated(channelId));

        //If there is a contact node, attempt to establish connection
        if (properties.containsKey("contact")) {
            try {
                String contact = properties.getProperty("contact");
                String[] hostElems = contact.split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                //We add to the pending set until the connection is successful
                //pending.add(contactHost);
                openConnection(contactHost);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + properties.getProperty("contacts"));
                e.printStackTrace();
                System.exit(-1);
            }
        }

        //Setup the timer used to send samples (we registered its handler on the constructor)
        setupPeriodicTimer(new SampleTimer(), this.sampleTime, this.sampleTime);

        //Setup the timer to display protocol information (also registered handler previously)
        int pMetricsInterval = Integer.parseInt(properties.getProperty("protocol_metrics_interval", "10000"));
        if (pMetricsInterval > 0)
            setupPeriodicTimer(new InfoTimer(), pMetricsInterval, pMetricsInterval);
    }





    private void uponFindSuccessor(FindSuccessorMessage msg, Host from, short sourceProto, int channelId) {
        BigInteger key = getPreviousOnFingerTable(msg.getOfNode());
        Host nodeToAsk;
        nodeToAsk = fingerTable.get(key);

        if (selfID.compareTo(HashGenerator.generateHash(msg.getOfNode().toString())) < 0) {
            //verifica se sucessor e maior ou igual ao que procuro
            BigInteger bigSuccessor = HashGenerator.generateHash(successor.toString());
            int cmp1 = bigSuccessor.compareTo(HashGenerator.generateHash(msg.getOfNode().toString()));
            //Verifica se o meu sucessor é menor que eu (caso de dar a volta ao anel)
            int cmp2 = bigSuccessor.compareTo(selfID);
            if (cmp1 >= 0 || cmp2 < 0) {
                //Trigger response
                //TODO RETURN successor
                //necessario abrir conexão
                SuccessorFoundMessage successorFoundMessage = new SuccessorFoundMessage(msg.getMid(), successor, msg.getOfNode(), msg.getToDeliver());
                sendMessage(successorFoundMessage, msg.getSender());
                //se nao pertencer a finger table fechar conexão
            } else {
                //TODO PEDIR AO FINGERtable ANTERIOR AO node findSuccessor
                if (nodeToAsk != null) {
                    sendMessage(msg, nodeToAsk);
                } else {
                    sendMessage(msg, successor);
                }
            }
        } else {
            //TODO PEDIR AO FINGERtable ANTERIOR AO node findSuccessor
            if (nodeToAsk != null) {
                sendMessage(msg, nodeToAsk);
            } else {
                sendMessage(msg, successor);
            }
        }

    }

    private void uponFoundSuccessor(SuccessorFoundMessage msg, Host from, short sourceProto, int channelId) {
        if (msg.getOfNode() == selfID) {
            successor = msg.getSuccessor();
            openConnection(successor);
            //TODO avisar sucessor que eu sou o predecessor,Notify
            //predecessor=from;
            NotificationMessage notify = new NotificationMessage(UUID.randomUUID(), self, PROTO_ID);
            sendMessage(notify, successor);
        } else {
            int cmp = HashGenerator.generateHash(from.toString()).compareTo(HashGenerator.generateHash(msg.getSuccessor().toString()));
            if (cmp > 0 && (msg.getOfNode().compareTo(HashGenerator.generateHash(msg.getSuccessor().toString())) > 0)) {
                //Refactor finger table, when completes ring cycle
                BigInteger offset = msg.getOfNode().subtract(HashGenerator.generateHash(from.toString()));
                fingerTable.remove(msg.getOfNode());
                fingerTable.put(offset, null);
                FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(UUID.randomUUID(), self, offset, PROTO_ID);
                sendMessage(findSuccessorMessage, from);
            } else {
                //adicionar a fingertable devera adicionar a entry(ofNode)
                BigInteger pos = getPreviousOnFingerTable(HashGenerator.generateHash(msg.getSuccessor().toString()));
                //fingerTable.put(msg.getOfNode(), msg.getSuccessor());
                fingerTable.put(pos, msg.getSuccessor());
                if (!connectedTo.contains(msg.getSuccessor())) {
                    openConnection(msg.getSuccessor());
                }
            }
        }
    }

    private void uponFindPredecessor(FindPredecessorMessage msg, Host from, short sourceProto, int channelId) {
        PredecessorFoundMessage predecessorFoundMessage = new PredecessorFoundMessage(msg.getMid(), predecessor, msg.getToDeliver());
        sendMessage(predecessorFoundMessage, msg.getSender());
    }

    private void uponFoundPredecessor(PredecessorFoundMessage msg, Host from, short sourceProto, int channelId) {
        //pode nao ser necessario, alternativa no caso de successor ser negativo, perguntar a rede pelo meu sucesor
        if (successor == null) {
            FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(UUID.randomUUID(), self, selfID, PROTO_ID);
            sendMessage(findSuccessorMessage, from);
        } else {
            Host peer = msg.getPredecessor();
            BigInteger bigPredecessor = HashGenerator.generateHash(peer.toString());
            BigInteger bigSuccessor = HashGenerator.generateHash(successor.toString());

            //my successor's predecessor is between me and my successor
            if (bigPredecessor.compareTo(selfID) > 0 && bigPredecessor.compareTo(bigSuccessor) < 0)
                successor = peer;
        }
            //notify my successor
            NotificationMessage notify = new NotificationMessage(UUID.randomUUID(), self, PROTO_ID);
            sendMessage(notify, successor);

    }


    private void uponLookUpRequestMessage(LookUpRequestMessage msg, Host from, short sourceProto, int channelId) {
        Host contentOwner = contentTable.get(msg.getContentHash());
        //se souber quem tem o ficheiro
        if(contentOwner != null){
            //retornar o owner do conteudo
            LookUpReplyMessage lookUpReplyMessage = new LookUpReplyMessage(UUID.randomUUID(),self,contentOwner,msg.getContentHash(),PROTO_ID);
            sendMessage(lookUpReplyMessage,msg.getSender());
        }else{
            BigInteger key =getPreviousOnFingerTable(msg.getContentHash());
            sendMessage(msg, fingerTable.get(key));
        }
    }

    private void uponLookUpReplyMessage(LookUpReplyMessage msg, Host from, short sourceProto, int channelId) {
        LookupReply lookupReply = new LookupReply(msg.getContentHash(), msg.getContentOwner(), UUID.randomUUID());
        sendReply(lookupReply,storageProtoId);
    }


    /* --------------------------------- Timer Events ---------------------------- */

    private void uponFixFinger(FixFingerTimer timer, long timerId) {
        BigInteger[] fingers = computeFingerNumbers(SIZE);
        HashMap<BigInteger, Host> auxFingerTable = new HashMap<BigInteger, Host>();
        for (BigInteger key : fingers) {
            BigInteger keyBigInteger = new BigInteger(String.valueOf(key));
            auxFingerTable.put(keyBigInteger, null);
            FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(UUID.randomUUID(), self, keyBigInteger, PROTO_ID);
            BigInteger val = getPreviousOnFingerTable(key);
            Host host = fingerTable.get(val);
            if (host == null) {
                sendMessage(findSuccessorMessage, successor);
            } else {
                sendMessage(findSuccessorMessage, host);
            }
        }
        fingerTable = auxFingerTable;
    }

    private void uponCheckPredecessor(CheckPredecessorTimer timer, long timerId) {
        //check if predecessor has failed
        if (connectedFrom.contains(predecessor)) {
            predecessor = null;
        }
    }

    private void uponStabilize(StabilizeTimer timer, long timerId) {
        //create message to successor asking for it's predecessor
        FindPredecessorMessage findPredecessorMessage = new FindPredecessorMessage(UUID.randomUUID(), self, PROTO_ID);
        sendMessage(findPredecessorMessage, successor);
    }

    private void uponNotify(NotificationMessage msg, Host from, short sourceProto, int channelId) {
        if (predecessor == null) {
            this.predecessor = msg.getSender();
        } else {
            BigInteger bigSender = HashGenerator.generateHash(msg.getSender().toString());
            BigInteger bigPredecessor = HashGenerator.generateHash(predecessor.toString());
            if (bigSender.compareTo(bigPredecessor) > 0 && bigSender.compareTo(selfID) < 0) {
                this.predecessor = msg.getSender();
            }
        }
    }

    /* --------------------------------- TCPChannel OUT Events ---------------------------- */

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} is down cause {}", peer, event.getCause());
        BigInteger bigPeer = HashGenerator.generateHash(peer.toString());
        //checks if the peer is my successor
        if (peer == successor) {
            BigInteger firstFingerKey = (BigInteger) fingerTable.keySet().toArray()[0];

            //checks if my first finger isn't the failed successor
            if (bigPeer.compareTo(firstFingerKey) == 0)
                firstFingerKey = (BigInteger) fingerTable.keySet().toArray()[1];

            Host firstFinger = fingerTable.get(firstFingerKey);
            FindSuccessorMessage findSuccessorMessage =
                    new FindSuccessorMessage(UUID.randomUUID(), self, firstFingerKey, PROTO_ID);
            sendMessage(findSuccessorMessage, firstFinger);
        }
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} failed cause: {}", event.getNode(), event.getCause());
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} is up", peer);
        connectedTo.add(peer);
        //Verificar se ID nao é conhecido peer!=fingertable
        if (successor == null) {
            UUID uuid = UUID.randomUUID();
            FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(uuid, self, selfID, PROTO_ID);
            sendMessage(findSuccessorMessage, peer);
            closeConnection(peer);
            connectedTo.remove(peer);
        }
    }

    /* --------------------------------- TCPChannel IN Events ---------------------------- */

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection from {} is up", peer);
        connectedFrom.add(peer);
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection from {} is down cause {}", peer, event.getCause());
        connectedFrom.remove(peer);
    }


    /*--------------------------------- Requests ---------------------------------------- */
    private void uponLookUpRequest(LookupRequest request, short sourceProto) {
        LookUpRequestMessage lookUpRequestMessage = new LookUpRequestMessage(UUID.randomUUID(),self,request.getID(),PROTO_ID);
        Host peer =fingerTable.get(getPreviousOnFingerTable(request.getID()));
        sendMessage(lookUpRequestMessage,peer);
    }

    /* --------------------------------- Aux------------------------------- */
    private BigInteger getPreviousOnFingerTable(BigInteger val) {
        BigInteger current = new BigInteger(String.valueOf(-1));
        BigInteger max = new BigInteger(String.valueOf(-1));
        List<BigInteger> keys = (List<BigInteger>) fingerTable.keySet();
        for (BigInteger key : keys) {
            if (key.compareTo(val) > 0)
                current = key;
            max = key;
        }
        if (current.equals(new BigInteger(String.valueOf(-1)))) {
            return max;
        } else {
            return current;
        }
    }

    private BigInteger[] computeFingerNumbers(int size) {
        BigInteger[] numbers = new BigInteger[size];
        for (int i = 0; i < size; i++) {
            int pow = (int) Math.pow(2, i);
            numbers[i] = selfID.add(new BigInteger(String.valueOf(pow)));
        }
        return numbers;
    }
}
