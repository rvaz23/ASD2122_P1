package protocols.apps.chord;

import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

public class ChordProtocol extends GenericProtocol {

    private static final String PROTO_NAME = "ChordApplication";
    private static final short PROTO_ID = 301;

    private ChordProtocol predecessor, successor;
    private long selfID;
    private HashMap<Long, ChordProtocol> fingerTable;
    private boolean hasFailed;
    private long next;

    //Variables related with measurement
    private long storeRequests = 0;
    private long storeRequestsCompleted = 0;
    private long retrieveRequests = 0;
    private long retrieveRequestsSuccessful = 0;
    private long retrieveRequestsFailed = 0;

    public ChordProtocol() {
        super(PROTO_NAME, PROTO_ID);
    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {

    }

    public void chordCreate() {
        predecessor = null;
        successor = this;
    }

    public void chordJoin(ChordProtocol n) {
        predecessor = null;
        successor = n.findSuccessor(this.selfID);
    }

    public void chordStabilized() {
        ChordProtocol x = successor.predecessor;
        if (x.selfID > this.selfID && x.selfID < this.successor.selfID)
            this.successor = x;

        successor.chordNotify(this);
    }

    public void chordNotify(ChordProtocol n) {
        if (this.predecessor == null || (n.selfID > predecessor.selfID && n.selfID < n.selfID))
            this.predecessor = n;
    }

    public void chordFixedFingers() {
        next = next + 1;
        if (next > fingerTable.size())
            next = 1;
        fingerTable.replace(next, findSuccessor(this.selfID + 2 ^ (next - 1)));
    }

    public void chordCheckPredecessor() {
        if (predecessor.hasFailed)
            predecessor = null;
    }

    public ChordProtocol findSuccessor(long id) {
        if (id > this.selfID && id <= successor.selfID)
            return successor;

        ChordProtocol n = closestPrecedingNode(id);
        return n.findSuccessor(id);
    }

    public ChordProtocol closestPrecedingNode(long id) {
        for (long i = fingerTable.size() - 1; i > 1; i--)
            if (id > this.selfID && fingerTable.get(i).selfID <= id)
                return fingerTable.get(i);

        return this;
    }

}
