package com.pristine.service.offermgmt.data;

import org.apache.log4j.Logger;

public interface WebServiceClient {
	
	public static Logger logger = Logger.getLogger("WebServiceClient");
	
	public String postRequest(String url, String input) throws Exception;
	
}
