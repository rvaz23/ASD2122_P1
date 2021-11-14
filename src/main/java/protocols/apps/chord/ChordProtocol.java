package protocols.apps.chord;

import channel.notifications.ChannelCreated;
import channel.notifications.ConnectionUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.apps.timers.InfoTimer;
import protocols.dht.ConnectionEntry;
import protocols.dht.messages.*;
import protocols.dht.replies.LookupReply;
import protocols.dht.requests.LookupRequest;
import protocols.dht.timers.CheckPredecessorTimer;
import protocols.dht.timers.FixFingerTimer;
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
    private static int SIZE = 1;

    //terlista de sucessores para otimizar a falha do successor

    private Host predecessor, successor;
    private BigInteger selfID;
    private boolean hasFailed;
    private short storageProtoId;
    private Host self;
    private final Map<Host, Long> connectedTo;                    //Peers I am connected to
    private final Set<Host> connectedFrom;                  //Peers connected to me
    private final HashMap<Host, Set<ProtoMessage>> pending; //Peers and respective pending messages to send
    private HashMap<BigInteger, ConnectionEntry> fingerTable;   //Peers that I know from finger table


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
        this.connectedTo = new HashMap<Host, Long>();
        this.connectedFrom = new HashSet<>();
        this.pending = new HashMap<Host, Set<ProtoMessage>>();
        this.selfID = HashGenerator.positiveBig(HashGenerator.generateHash(self.toString()));
        System.out.println(selfID);

        this.fingerTable = new HashMap<BigInteger, ConnectionEntry>();
        SIZE = Integer.parseInt(properties.getProperty("finger_size", "5"));
        /*for (BigInteger finger : computeFingerNumbers(SIZE)) {
            fingerTable.put(new BigInteger(String.valueOf(finger)), null);
        }*/
        predecessor = null;
        successor = null;


        //Get some configurations from the Properties object
        this.fixTime = Integer.parseInt(properties.getProperty("fixFingers_time", "10000"));     //2 seconds
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
        ConnectionEntry nodeToAsk = fingerTable.get(key);
        logger.debug("Find message received id:{} ,from:{}", msg.getMid(), msg.getSender());

        if (nodeToAsk == null && successor == null) {
            SuccessorFoundMessage successorFoundMessage = new SuccessorFoundMessage(msg.getMid(), self, msg.getOfNode(), msg.getToDeliver());
            updateConnected(msg.getSender());
            trySendMessage(successorFoundMessage, msg.getSender());
            successor = msg.getSender();
            NotificationMessage notificationMessage = new NotificationMessage(UUID.randomUUID(), self, sourceProto);
            trySendMessage(notificationMessage, successor);
            return;
        }
        BigInteger bigSuccessor = HashGenerator.positiveBig(HashGenerator.generateHash(successor.toString()));
        BigInteger BigNode =msg.getOfNode();

        //self < que novo
        if (selfID.compareTo(BigNode) < 0) {
            logger.debug("self :{} ,< :{}", selfID, BigNode);
            //checks if my successor >= msg.getOfNode
            int cmp1 = bigSuccessor.compareTo(BigNode);
            //checks if my successor >= myself
            int cmp2 = bigSuccessor.compareTo(selfID);

            if (bigSuccessor.compareTo(BigNode) > 0 || bigSuccessor.compareTo(selfID) < 0) {
                logger.debug("cmp1 && cmp2 :{} ,< :{}", successor, msg.getOfNode());
                SuccessorFoundMessage successorFoundMessage = new SuccessorFoundMessage(msg.getMid(), successor, msg.getOfNode(), msg.getToDeliver());
                if(msg.getSender().equals(self)){
                    uponFoundSuccessor(successorFoundMessage,self,PROTO_ID,channelId);
                }else{
                    updateConnected(msg.getSender());
                    trySendMessage(successorFoundMessage, msg.getSender());
                    return;
                }
            }
        } else {
            logger.debug("self :{} ,> :{}", self, BigNode);
            if (selfID.compareTo(bigSuccessor) > 0 && BigNode.compareTo(bigSuccessor) < 0) {
                logger.debug("cmp3 && cmp4 :{} ,< :{}", successor, msg.getOfNode());
                SuccessorFoundMessage successorFoundMessage = new SuccessorFoundMessage(msg.getMid(), successor, msg.getOfNode(), msg.getToDeliver());
                updateConnected(msg.getSender());
                trySendMessage(successorFoundMessage, msg.getSender());
                return;
            }
        }

        if (nodeToAsk != null) {
            logger.debug("resend to finger {} ", nodeToAsk.getPeer());
            updateConnected(nodeToAsk.getPeer());
            trySendMessage(msg, nodeToAsk.getPeer());
        } else {
            logger.debug("resend to suc {} ", successor);
            updateConnected(successor);
            trySendMessage(msg, successor);
        }


        //sucessor > novo
                /*if (cmp1 >= 0 || cmp2 < 0) {
                    logger.info("Find Successor para {} com {}", msg.getSender(), successor);
                    SuccessorFoundMessage successorFoundMessage = new SuccessorFoundMessage(msg.getMid(), successor, msg.getOfNode(), msg.getToDeliver());
                    if (msg.getSender() == self) {
                        uponFoundSuccessor(successorFoundMessage, self, PROTO_ID, channelId);
                    } else {

                        updateConnected(msg.getSender());
                        trySendMessage(successorFoundMessage, msg.getSender());
                    }
                    //se nao pertencer a finger table fechar conexão
                } else {
                    //TODO PEDIR AO FINGERtable ANTERIOR AO node findSuccessor
                    if (nodeToAsk != null) {
                        updateConnected(nodeToAsk.getPeer());
                        trySendMessage(msg, nodeToAsk.getPeer());
                    } else {
                        updateConnected(successor);
                        trySendMessage(msg, successor);
                    }
                }
            } else {
                //Novo < self
                BigInteger bigSuccessor = HashGenerator.positiveBig(HashGenerator.generateHash(successor.toString()));
                BigInteger BigNode = HashGenerator.positiveBig(HashGenerator.generateHash(msg.getOfNode().toString()));
                int cmp1 = BigNode.compareTo(selfID);
                //novo < successor
                int cmp2 = bigSuccessor.compareTo(BigNode);
                if (cmp1 < 0 && cmp2 > 0) {
                    logger.info("Find Successor para {} com {}", msg.getSender(), successor);
                    SuccessorFoundMessage successorFoundMessage = new SuccessorFoundMessage(msg.getMid(), successor, msg.getOfNode(), msg.getToDeliver());
                    if (msg.getSender() == self) {
                        uponFoundSuccessor(successorFoundMessage, self, PROTO_ID, channelId);
                    } else {

                        updateConnected(msg.getSender());
                        trySendMessage(successorFoundMessage, msg.getSender());
                    }
                } else {
                    if (nodeToAsk != null) {
                        updateConnected(nodeToAsk.getPeer());
                        trySendMessage(msg, nodeToAsk.getPeer());
                    } else {
                        updateConnected(successor);
                        trySendMessage(msg, successor);
                    }
                }
            }*/


    }


    private void uponFoundSuccessor(SuccessorFoundMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Found Successor {} of node {}", msg.getSuccessor(), msg.getOfNode());
        if (msg.getOfNode().equals(selfID)) {
            successor = msg.getSuccessor();
            updateConnected(successor);
            //TODO avisar sucessor que eu sou o predecessor,Notify
            //predecessor=from;
            NotificationMessage notify = new NotificationMessage(UUID.randomUUID(), self, sourceProto);
            trySendMessage(notify, successor);
            logger.debug("Found my Successor {} ", successor);
            return;
        }


            int cmp = HashGenerator.positiveBig(HashGenerator.generateHash(from.toString())).compareTo(HashGenerator.positiveBig(HashGenerator.generateHash(msg.getSuccessor().toString())));
            if (cmp > 0 && (msg.getOfNode().compareTo(HashGenerator.positiveBig(HashGenerator.generateHash(from.toString()))) > 0)) {

                //Refactor finger table, when completes ring cycle
                BigInteger index = msg.getOfNode().remainder(HashGenerator.positiveBig(HashGenerator.generateHash(from.toString())));
                logger.info("Remap do {} , to {}", msg.getOfNode(), index);
                FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(UUID.randomUUID(), self, index, PROTO_ID);
                updateConnected(from);
                trySendMessage(findSuccessorMessage, from);
            } else {
                logger.info("Inserting finger do {} , to {}", msg.getOfNode(), msg.getSuccessor());
                //adicionar a fingertable devera adicionar a entry(ofNode)
                BigInteger pos = getPreviousOnFingerTable(HashGenerator.positiveBig(HashGenerator.generateHash(msg.getSuccessor().toString())));
                //
                ConnectionEntry connectionEntry = new ConnectionEntry(System.currentTimeMillis(), msg.getSuccessor());
                fingerTable.put(msg.getOfNode(), connectionEntry);
                //fingerTable.put(pos, connectionEntry);
                updateConnected(msg.getSuccessor());
            }

    }

    private void uponFindPredecessor(FindPredecessorMessage msg, Host from, short sourceProto, int channelId) {
        PredecessorFoundMessage predecessorFoundMessage = new PredecessorFoundMessage(msg.getMid(), predecessor, msg.getToDeliver());
        updateConnected(msg.getSender());
        trySendMessage(predecessorFoundMessage, msg.getSender());
    }

    private void uponFoundPredecessor(PredecessorFoundMessage msg, Host from, short sourceProto, int channelId) {

        if (msg.getPredecessor().equals(self))
            return;

        //pode nao ser necessario, alternativa no caso de successor ser negativo, perguntar a rede pelo meu sucesor
        if (successor == null) {
            updateConnected(from);
            FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(UUID.randomUUID(), self, selfID, PROTO_ID);
            trySendMessage(findSuccessorMessage, from);
        } else {
            Host peer = msg.getPredecessor();
            BigInteger bigPredecessor = HashGenerator.positiveBig(HashGenerator.generateHash(peer.toString()));
            BigInteger bigSuccessor = HashGenerator.positiveBig(HashGenerator.generateHash(successor.toString()));

            if (bigPredecessor.compareTo(bigSuccessor) < 0 && bigSuccessor.compareTo(selfID) > 0) {
                successor = peer;
            } else if (bigSuccessor.compareTo(selfID) < 0) {
                if (bigPredecessor.compareTo(selfID) > 0 || bigPredecessor.compareTo(bigSuccessor) < 0) {
                    successor = peer;
                }
            }

        }
        //notify my successor
        NotificationMessage notify = new NotificationMessage(UUID.randomUUID(), self, PROTO_ID);
        updateConnected(successor);
        trySendMessage(notify, successor);
    }

    private void uponLookUpRequestMessage(LookUpRequestMessage msg, Host from, short sourceProto, int channelId) {
        if (selfID.compareTo(HashGenerator.positiveBig(HashGenerator.generateHash(msg.getContentHash().toString()))) < 0) {
            BigInteger bigSuccessor = HashGenerator.positiveBig(HashGenerator.generateHash(successor.toString()));
            int cmp1 = bigSuccessor.compareTo(HashGenerator.positiveBig(HashGenerator.generateHash(msg.getContentHash().toString())));
            int cmp2 = bigSuccessor.compareTo(selfID);
            if (cmp1 >= 0 || cmp2 < 0) {
                LookUpReplyMessage lookUpReplyMessage = new LookUpReplyMessage(UUID.randomUUID(), self, self, msg.getContentHash(), PROTO_ID);
                updateConnected(msg.getSender());
                trySendMessage(lookUpReplyMessage, msg.getSender());
                return;
            }
        }
        BigInteger key = getPreviousOnFingerTable(msg.getContentHash());
        trySendMessage(msg, fingerTable.get(key).getPeer());
    }

    private void uponLookUpReplyMessage(LookUpReplyMessage msg, Host from, short sourceProto, int channelId) {
        LookupReply lookupReply = new LookupReply(msg.getContentHash(), msg.getContentOwner(), UUID.randomUUID());
        updateConnected(msg.getContentOwner());
        sendReply(lookupReply, storageProtoId);
    }

    /* --------------------------------- Timer Events ---------------------------- */

    //TODO Faz sentido fazer update da conexao na fix fingers
    //Todo ao atualizar nunca é removido se ja não for o correto
    private void uponFixFinger(protocols.dht.timers.FixFingerTimer timer, long timerId) {
        BigInteger[] fingers = computeFingerNumbers(SIZE);
        if (!fingerTable.isEmpty() || successor != null) {
            for (BigInteger key : fingers) {
                BigInteger keyBigInteger = new BigInteger(String.valueOf(key));
                FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(UUID.randomUUID(), self, keyBigInteger, PROTO_ID);
                BigInteger val = getPreviousOnFingerTable(key);
                ConnectionEntry host = fingerTable.get(val);
                if (host == null)
                    trySendMessage(findSuccessorMessage, successor);
                else
                    trySendMessage(findSuccessorMessage, host.getPeer());

            }
        }

        //clean old entries on fingertable
        long currentTime = System.currentTimeMillis();

        if (!fingerTable.isEmpty()) {
            for (BigInteger fkey : fingerTable.keySet()) {
                ConnectionEntry connectionEntry = fingerTable.get(fkey);
                System.out.println(fkey + ": " + connectionEntry.peer.toString());
                if (currentTime - connectionEntry.getLastTimeRead() > 10 * fixTime) {
                    //closeConnection(connectionEntry.getPeer());
                    //fingerTable.remove(fkey);
                }
            }
        }

    }

    private void uponCheckPredecessor(CheckPredecessorTimer timer, long timerId) {
        if (predecessor != null) {
            logger.info("CheckPredecessor equals: {}", predecessor.toString());
            //check if predecessor has failed
            if (!connectedFrom.contains(predecessor)) {
                predecessor = null;
            }
        }
    }

    private void uponStabilize(StabilizeTimer timer, long timerId) {
        if (successor != null) {
            logger.info("Stabilize, successor:{}", successor.toString());
            //create message to successor asking for it's predecessor
            FindPredecessorMessage findPredecessorMessage = new FindPredecessorMessage(UUID.randomUUID(), self, PROTO_ID);
            updateConnected(successor);
            trySendMessage(findPredecessorMessage, successor);
        }

    }

    private void uponNotify(NotificationMessage msg, Host from, short sourceProto, int channelId) {
        if (predecessor == null) {
            this.predecessor = msg.getSender();
        } else {
            BigInteger bigSender = HashGenerator.positiveBig(HashGenerator.generateHash(msg.getSender().toString()));
            BigInteger bigPredecessor = HashGenerator.positiveBig(HashGenerator.generateHash(predecessor.toString()));

            if (bigPredecessor.compareTo(selfID) < 0 && bigSender.compareTo(bigPredecessor) > 0 && bigSender.compareTo(selfID) < 0)
                this.predecessor = msg.getSender();
            else if (bigPredecessor.compareTo(selfID) > 0) {
                if (bigSender.compareTo(selfID) < 0 || bigSender.compareTo(bigPredecessor) > 0)
                    this.predecessor = msg.getSender();
            }


        }
        logger.info("Notify from {}, precessor = {}", msg.getSender(), predecessor);
    }

    /* --------------------------------- TCPChannel OUT Events ---------------------------- */

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} is down cause {}", peer, event.getCause());
        BigInteger bigPeer = HashGenerator.positiveBig(HashGenerator.generateHash(peer.toString()));
        //have list of successors makes it easier
        //checks if the peer is my successor
        if (peer.equals(successor)) {
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
        if (connectedTo.put(peer, System.currentTimeMillis()) != null) {
            triggerNotification(new ConnectionUp(peer));
        }


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
            //ConnectionEntry connectionEntry = new ConnectionEntry(System.currentTimeMillis(), peer);
            // fingerTable.put(selfID.add(new BigInteger("1")), connectionEntry);
            //successor = peer;
        }

        //if new peer not in finger, close connection
        boolean fingerTableContainsPeer = false;
        for (ConnectionEntry connectionEntry : fingerTable.values()) {
            if (connectionEntry.peer.equals(peer)) {
                fingerTableContainsPeer = true;
                break;
            }
        }
        //pode nao conseguir enviar a msg e acaba por perder, no caso por exemplo lookupreply
        //if (!fingerTableContainsPeer)
        //closeConnection(peer);
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
        if (selfID.compareTo(HashGenerator.positiveBig(HashGenerator.generateHash(request.getID().toString()))) < 0) {
            BigInteger bigSuccessor = HashGenerator.positiveBig(HashGenerator.generateHash(successor.toString()));
            int cmp1 = bigSuccessor.compareTo(HashGenerator.positiveBig(HashGenerator.generateHash(request.getID().toString())));
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
        Set<BigInteger> keys = fingerTable.keySet();
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
        if (connectedTo.containsKey(destination)) {
            logger.debug("Sending to {} message", destination);
            //there's an open connection to the destination
            sendMessage(message, destination);
        } else {
            //there's no open connection to the destination. need to open one
            Set<ProtoMessage> pendingMessages = pending.get(destination);
            if (pendingMessages == null)
                pendingMessages = new HashSet<>();
            pendingMessages.add(message);
            pending.put(destination, pendingMessages);

        }
        updateConnected(destination);
    }

    private BigInteger[] computeFingerNumbers(int size) {
        BigInteger[] numbers = new BigInteger[size];
        for (int i = 0; i < size; i++) {
            int pow = (int) Math.pow(2, i);
            numbers[i] = selfID.add(new BigInteger(String.valueOf(pow)));
        }
        return numbers;
    }

    private void updateConnected(Host peer) {
        if (!connectedTo.containsKey(peer)) {
            openConnection(peer);
        }
        connectedTo.put(peer, System.currentTimeMillis());

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
