package protocols.storage.replies;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoReply;

public class StoreOKReply extends ProtoReply {

	final public static short REPLY_ID = 203;
	
	private String name;
	private UUID uid;
	
	public StoreOKReply(String name, UUID uid) {
		super(StoreOKReply.REPLY_ID);
		this.name = name;
		this.uid = uid;
	}
	
	public UUID getReplyUID() {
		return this.uid;
	}
	
	public String getName() {
		return this.name;
	}
	
}
