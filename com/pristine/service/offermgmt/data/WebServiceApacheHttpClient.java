package com.pristine.service.offermgmt.data;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class WebServiceApacheHttpClient implements WebServiceClient{

	public WebServiceApacheHttpClient() {
		Logger.getLogger("org.apache.commons.httpclient").setLevel(Level.WARN);
		Logger.getLogger("org.apache.http").setLevel(Level.WARN);
	}
	
	@Override
	public String postRequest(String url, String input) throws Exception {
		String responseJson = null;
		CloseableHttpClient httpclient = HttpClients.createDefault();
		HttpPost httpPost = new HttpPost(url);
		httpPost.setHeader("Accept", "application/json");
		httpPost.setHeader("Content-type", "application/json");

		StringEntity stringEntity = new StringEntity(input);
		httpPost.setEntity(stringEntity);
		System.out.println("Executing request " + httpPost.getRequestLine());
		HttpResponse response = httpclient.execute(httpPost);
		//BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));

		if (response.getStatusLine().getStatusCode() == 200) {
			// Create the StringBuffer object and store the response into it.
			responseJson = IOUtils.toString(response.getEntity().getContent());
			//StringBuffer result = new StringBuffer();
			//String line = "";
			//while ((line = br.readLine()) != null) {
				//result.append(line);
			//}
			//br.close();
			responseJson = responseJson.toString().replaceAll("\\\\", "").replaceAll("\"\\[", "[").replaceAll("\\]\"", "]");
			//logger.debug("Response: " + responseJson);
		} else {
			logger.error("Error - Failure Response Code: {}" + response.getStatusLine().getStatusCode() + ", input: "
					+ input);
			throw new Exception("Error - Failure Response Code: {}" + response.getStatusLine().getStatusCode()
					+ ", input: " + input);
		}

		return responseJson;
	}
}
