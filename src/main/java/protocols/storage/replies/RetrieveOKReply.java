package protocols.storage.replies;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoReply;

public class RetrieveOKReply extends ProtoReply {

	final public static short REPLY_ID = 204;
	
	private String name;
	private byte[] content;
	private UUID uid;
	
	public RetrieveOKReply(String name, UUID uid, byte[] content) {
		super(RetrieveOKReply.REPLY_ID);
		this.name = name;
		this.content = content;
		this.uid = uid;
	}
	
	public UUID getReplyUID() {
		return this.uid;
	}
	
	public String getName() {
		return this.name;
	}
	
	public byte[] getContent() {
		return this.content;
	}
}
