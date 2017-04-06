package com.netflix.evcache.service.resources;

/**
 * Created by senugula on 4/6/17.
 */
public class BulkQueue  implements Runnable {

    private final String appId;
    private final String input;

    public BulkQueue(String appId, String input) {
		super();
		this.appId = appId;
		this.input = input;
	}


	public String getAppId() {
        return appId;
    }


    public String getInput() {
        return input;
    }



	@Override
	public void run() {
	}
}
