package protocols.apps.chord;

import channel.notifications.ChannelCreated;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.apps.timers.InfoTimer;
import protocols.apps.timers.FixFingerTimer;
import protocols.dht.FingerEntry;
import protocols.dht.messages.*;
import protocols.dht.replies.LookupReply;
import protocols.dht.requests.LookupRequest;
import protocols.dht.timers.CheckPredecessorTimer;
import protocols.dht.timers.StabilizeTimer;
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
    private static int SIZE = 5;


    private Host predecessor, successor;
    private BigInteger selfID;
    private boolean hasFailed;
    private short storageProtoId;
    private Host self;
    private final Set<Host> connectedTo;                    //Peers I am connected to
    private final Set<Host> connectedFrom;                  //Peers connected to me
    private final HashMap<Host, Set<ProtoMessage>> pending; //Peers and respective pending messages to send
    private HashMap<BigInteger, FingerEntry> fingerTable;   //Peers that I know from finger table


    private final int fixTime;  //param: timeout for fixFingersUpdate
    private final int stabTime; //param: timeout for fixFingersUpdate
    private final int predTime; //param: timeout for fixFingersUpdate


    //Variables related with measurement
    private long storeRequests = 0;
    private long storeRequestsCompleted = 0;
    private long retrieveRequests = 0;
    private long retrieveRequestsSuccessful = 0;
    private long retrieveRequestsFailed = 0;

    private final int channelId; //ID of the created channel

    public ChordProtocol(Properties properties, Host self, short storageProtoId) throws IOException, HandlerRegistrationException {
        super(PROTO_NAME, PROTO_ID);
        this.self = self;
        this.storageProtoId = storageProtoId;
        this.connectedTo = new HashSet<>();
        this.connectedFrom = new HashSet<>();
        this.pending = new HashMap<Host, Set<ProtoMessage>>();
        this.selfID = HashGenerator.generateHash(self.toString());
        this.fingerTable = new HashMap<BigInteger, FingerEntry>();
        SIZE = Integer.parseInt(properties.getProperty("finger_size", "5"));
        /*for (BigInteger finger : computeFingerNumbers(SIZE)) {
            fingerTable.put(new BigInteger(String.valueOf(finger)), null);
        }*/
        predecessor = null;
        successor = null;


        //Get some configurations from the Properties object
        this.fixTime = Integer.parseInt(properties.getProperty("fixFingers_time", "2000"));     //2 seconds
        this.stabTime = Integer.parseInt(properties.getProperty("stabilize_time", "2000"));     //2 seconds
        this.predTime = Integer.parseInt(properties.getProperty("predecessor_time", "2000"));   //2 seconds

        String cMetricsInterval = properties.getProperty("channel_metrics_interval", "10000");  //10 seconds

        //Create a properties object to setup channel-specific properties. See the channel description for more details.
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, properties.getProperty("address"));//The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, properties.getProperty("port"));      //The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval);        //The interval to receive channel metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");                //Heartbeats interval for established connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");               //Time passed without heartbeats until closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");                   //TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps);                           //Create the channel with the given properties

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
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
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
                openConnection(contactHost);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + properties.getProperty("contacts"));
                e.printStackTrace();
                System.exit(-1);
            }
        }

        //Set up the timer used to send samples (we registered its handler on the constructor)
        setupPeriodicTimer(new FixFingerTimer(), this.fixTime, this.fixTime);
        setupPeriodicTimer(new StabilizeTimer(), this.stabTime, this.stabTime);
        setupPeriodicTimer(new CheckPredecessorTimer(), this.predTime, this.predTime);

        //Set up the timer to display protocol information (also registered handler previously)
        int pMetricsInterval = Integer.parseInt(properties.getProperty("protocol_metrics_interval", "10000"));
        if (pMetricsInterval > 0)
            setupPeriodicTimer(new InfoTimer(), pMetricsInterval, pMetricsInterval);
    }


    private void uponFindSuccessor(FindSuccessorMessage msg, Host from, short sourceProto, int channelId) {
        BigInteger key = getPreviousOnFingerTable(msg.getOfNode());
        Host nodeToAsk;
        nodeToAsk = fingerTable.get(key).getPeer();

        if (selfID.compareTo(HashGenerator.generateHash(msg.getOfNode().toString())) < 0) {
            //checks if my successor >= msg.getOfNode
            BigInteger bigSuccessor = HashGenerator.generateHash(successor.toString());
            int cmp1 = bigSuccessor.compareTo(HashGenerator.generateHash(msg.getOfNode().toString()));
            //checks if my successor >= myself
            int cmp2 = bigSuccessor.compareTo(selfID);
            if (cmp1 >= 0 || cmp2 < 0) {
                //Trigger response
                //TODO RETURN successor
                //necessario abrir conexão
                SuccessorFoundMessage successorFoundMessage = new SuccessorFoundMessage(msg.getMid(), successor, msg.getOfNode(), msg.getToDeliver());
                trySendMessage(successorFoundMessage, msg.getSender());
                //se nao pertencer a finger table fechar conexão
            } else {
                //TODO PEDIR AO FINGERtable ANTERIOR AO node findSuccessor
                if (nodeToAsk != null) {
                    trySendMessage(msg, nodeToAsk);
                } else {
                    trySendMessage(msg, successor);
                }
            }
        } else {
            //TODO PEDIR AO FINGERtable ANTERIOR AO node findSuccessor
            if (nodeToAsk != null) {
                trySendMessage(msg, nodeToAsk);
            } else {
                trySendMessage(msg, successor);
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
            trySendMessage(notify, successor);
        } else {
            int cmp = HashGenerator.generateHash(from.toString()).compareTo(HashGenerator.generateHash(msg.getSuccessor().toString()));
            if (cmp > 0 && (msg.getOfNode().compareTo(HashGenerator.generateHash(msg.getSuccessor().toString())) > 0)) {
                //Refactor finger table, when completes ring cycle
                BigInteger index = msg.getOfNode().remainder(HashGenerator.generateHash(from.toString()));
                //BigInteger offset = msg.getOfNode().subtract());
                fingerTable.remove(msg.getOfNode());
                //fingerTable.put(index, null);
                FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(UUID.randomUUID(), self, index, PROTO_ID);
                trySendMessage(findSuccessorMessage, from);
            } else {
                //adicionar a fingertable devera adicionar a entry(ofNode)
                BigInteger pos = getPreviousOnFingerTable(HashGenerator.generateHash(msg.getSuccessor().toString()));
                //fingerTable.put(msg.getOfNode(), msg.getSuccessor());
                FingerEntry fingerEntry = new FingerEntry(System.currentTimeMillis(), msg.getSuccessor());
                fingerTable.put(pos, fingerEntry);
                if (!connectedTo.contains(msg.getSuccessor())) {
                    openConnection(msg.getSuccessor());
                }
            }
        }
    }

    private void uponFindPredecessor(FindPredecessorMessage msg, Host from, short sourceProto, int channelId) {
        PredecessorFoundMessage predecessorFoundMessage = new PredecessorFoundMessage(msg.getMid(), predecessor, msg.getToDeliver());
        trySendMessage(predecessorFoundMessage, msg.getSender());
    }

    private void uponFoundPredecessor(PredecessorFoundMessage msg, Host from, short sourceProto, int channelId) {
        //pode nao ser necessario, alternativa no caso de successor ser negativo, perguntar a rede pelo meu sucesor
        if (successor == null) {
            FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(UUID.randomUUID(), self, selfID, PROTO_ID);
            trySendMessage(findSuccessorMessage, from);
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
        trySendMessage(notify, successor);
    }

    private void uponLookUpRequestMessage(LookUpRequestMessage msg, Host from, short sourceProto, int channelId) {
        if (selfID.compareTo(HashGenerator.generateHash(msg.getContentHash().toString())) < 0) {
            BigInteger bigSuccessor = HashGenerator.generateHash(successor.toString());
            int cmp1 = bigSuccessor.compareTo(HashGenerator.generateHash(msg.getContentHash().toString()));
            int cmp2 = bigSuccessor.compareTo(selfID);
            if (cmp1 >= 0 || cmp2 < 0) {
                LookUpReplyMessage lookUpReplyMessage = new LookUpReplyMessage(UUID.randomUUID(), self, self, msg.getContentHash(), PROTO_ID);
                trySendMessage(lookUpReplyMessage, msg.getSender());
                return;
            }
        }
        BigInteger key = getPreviousOnFingerTable(msg.getContentHash());
        trySendMessage(msg, fingerTable.get(key).getPeer());
    }

    private void uponLookUpReplyMessage(LookUpReplyMessage msg, Host from, short sourceProto, int channelId) {
        LookupReply lookupReply = new LookupReply(msg.getContentHash(), msg.getContentOwner(), UUID.randomUUID());
        sendReply(lookupReply, storageProtoId);
    }

    /* --------------------------------- Timer Events ---------------------------- */

    private void uponFixFinger(protocols.dht.timers.FixFingerTimer timer, long timerId) {
        BigInteger[] fingers = computeFingerNumbers(SIZE);
        for (BigInteger key : fingers) {
            BigInteger keyBigInteger = new BigInteger(String.valueOf(key));
            FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(UUID.randomUUID(), self, keyBigInteger, PROTO_ID);
            BigInteger val = getPreviousOnFingerTable(key);
            Host host = fingerTable.get(val).getPeer();
            if (host == null)
                trySendMessage(findSuccessorMessage, successor);
            else
                trySendMessage(findSuccessorMessage, host);

        }
        //clean old entries on fingertable
        long currentTime = System.currentTimeMillis();
        for (BigInteger fkey : fingerTable.keySet()) {
            FingerEntry fingerEntry = fingerTable.get(fkey);
            if (currentTime - fingerEntry.getLastTimeRead() > 2 * fixTime) {
                closeConnection(fingerEntry.getPeer());
                fingerTable.remove(fkey);
            }
        }
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
        trySendMessage(findPredecessorMessage, successor);
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

            Host firstFinger = fingerTable.get(firstFingerKey).getPeer();
            FindSuccessorMessage findSuccessorMessage =
                    new FindSuccessorMessage(UUID.randomUUID(), self, firstFingerKey, PROTO_ID);
            trySendMessage(findSuccessorMessage, firstFinger);
        }
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} failed cause: {}", peer, event.getCause());
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} is up", peer);
        connectedTo.add(peer);

        //checks if there's pending messages to this new peer
        Set<ProtoMessage> pendingMessages = pending.get(peer);
        if (pendingMessages != null && !pendingMessages.isEmpty()) {
            for (ProtoMessage protoMessage : pendingMessages)
                trySendMessage(protoMessage, peer);
            pendingMessages.remove(peer);
        }

        //Verificar se ID nao é conhecido peer!=fingertable
        if (successor == null) {
            UUID uuid = UUID.randomUUID();
            FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(uuid, self, selfID, PROTO_ID);
            trySendMessage(findSuccessorMessage, peer);
            FingerEntry fingerEntry = new FingerEntry(System.currentTimeMillis(), peer);
            fingerTable.put(selfID.add(new BigInteger("1")), fingerEntry);
            successor = peer;
        }

        //if new peer not in finger, close connection
        boolean fingerTableContainsPeer = false;
        for (FingerEntry fingerEntry : fingerTable.values()) {
            if (fingerEntry.peer.equals(peer)) {
                fingerTableContainsPeer = true;
                break;
            }
        }
        if (!fingerTableContainsPeer)
            closeConnection(peer);
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
        if (selfID.compareTo(HashGenerator.generateHash(request.getID().toString())) < 0) {
            BigInteger bigSuccessor = HashGenerator.generateHash(successor.toString());
            int cmp1 = bigSuccessor.compareTo(HashGenerator.generateHash(request.getID().toString()));
            int cmp2 = bigSuccessor.compareTo(selfID);
            if (cmp1 >= 0 || cmp2 < 0) {
                LookupReply lookupReply = new LookupReply(request.getID(), self, UUID.randomUUID());
                sendReply(lookupReply, storageProtoId);
                return;
            }
        }
        LookUpRequestMessage lookUpRequestMessage = new LookUpRequestMessage(UUID.randomUUID(), self, request.getID(), PROTO_ID);
        Host peer = fingerTable.get(getPreviousOnFingerTable(request.getID())).getPeer();
        trySendMessage(lookUpRequestMessage, peer);
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

    private void trySendMessage(ProtoMessage message, Host destination) {
        if (connectedTo.contains(destination))
            //there's an open connection to the destination
            sendMessage(message, destination);
        else {
            //there's no open connection to the destination. need to open one
            Set<ProtoMessage> pendingMessages = pending.get(destination);
            if (pendingMessages == null)
                pendingMessages = new HashSet<>();
            pendingMessages.add(message);
            pending.put(destination, pendingMessages);
            openConnection(destination);
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

    private void uponChannelMetrics(ChannelMetrics event, int channelId) {
        StringBuilder sb = new StringBuilder("Channel Metrics:\n");
        sb.append("In channels:\n");
        event.getInConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        event.getOldInConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        sb.append("Out channels:\n");
        event.getOutConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        event.getOldOutConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        sb.setLength(sb.length() - 1);
        logger.info(sb);
    }
}
