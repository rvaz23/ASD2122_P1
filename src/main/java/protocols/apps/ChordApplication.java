package protocols.apps;

import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.internal.InternalEvent;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public class ChordApplication extends GenericProtocol {

    private static final String PROTO_NAME = "ChordApplication";
    private static final short PROTO_ID = 301;

    private long predecessor, successor;
    private HashMap<Long, InetAddress> fingerTable;

    //Variables related with measurement
    private long storeRequests = 0;
    private long storeRequestsCompleted = 0;
    private long retrieveRequests = 0;
    private long retrieveRequestsSuccessful = 0;
    private long retrieveRequestsFailed = 0;

    public ChordApplication() {
        super(PROTO_NAME, PROTO_ID);
    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {

    }

    public void chordCreate() {

    }

    public void chordJoin() {

    }

    public void chordStabilized() {

    }

    public void chordNotify() {

    }

    public void chordFixedFingers() {

    }

    public void chordCheckPredecessor() {

    }

}
