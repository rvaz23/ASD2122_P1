package protocols.storage.replies;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoReply;

public class RetrieveFailedReply extends ProtoReply {

	final public static short REPLY_ID = 205;
	
	private String name;
	private UUID uid;
	
	public RetrieveFailedReply(String name, UUID uid) {
		super(RetrieveFailedReply.REPLY_ID);
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
