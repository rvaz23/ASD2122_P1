package protocols.storage.requests;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class RetrieveRequest extends ProtoRequest {

	final public static short REQUEST_ID = 201;
	
	private String name;
	private UUID uid;
	
	public RetrieveRequest(String name) {
		super(RetrieveRequest.REQUEST_ID);
		this.name = name;
		this.uid = UUID.randomUUID();
	}
	
	public UUID getRequestUID() {
		return this.uid;
	}
	
	public String getName() {
		return this.name;
	}

}
