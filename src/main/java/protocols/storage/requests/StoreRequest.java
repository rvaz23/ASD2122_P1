package protocols.storage.requests;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class StoreRequest extends ProtoRequest {

	final public static short REQUEST_ID = 202;
	
	private String name;
	private byte[] content;
	private UUID uid;
	
	public StoreRequest(String name, byte[] content) {
		super(StoreRequest.REQUEST_ID);
		this.name = name;
		this.content = content;
		this.uid = UUID.randomUUID();
	}
	
	public UUID getRequestUID() {
		return this.uid;
	}
	
	public String getName() {
		return this.name;
	}
	
	public byte[] getContent() {
		return this.content;
	}

}
