/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.pristine.util;

/**
 *
 * @author vaibhavkumar
 */
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import org.apache.log4j.Logger;

public class BaseXMLParser {

	private static String baseurl = null;
	private static String encodedstring = null;
	static Logger logger	= Logger.getLogger(BaseXMLParser.class);
	
	public InputStream getHTTPResponse(String url) throws Exception{
		HttpURLConnection urlConn = null;
		//String url = "http://localhost:8082/PrestoAnalytics/xml/"+xmlFile;
		logger.info(" URl is >>"+url);
		URL urlobject = new URL(url);

		urlConn =(HttpURLConnection)urlobject.openConnection();
		urlConn.setDoOutput(true);
                //urlConn.setRequestProperty("authorization", encodedstring);
		OutputStreamWriter wr = new OutputStreamWriter(urlConn.getOutputStream());
		wr.flush();
		wr.close();
		return urlConn.getInputStream();
	}

	public static String getBaseurl() {
		return baseurl;
	}

	public static void setBaseurl(String baseurl) {
		BaseXMLParser.baseurl = baseurl;
	}

	public static String getEncodedstring() {
		return encodedstring;
	}

	public static void setEncodedstring(String encodedstring) {
		BaseXMLParser.encodedstring = encodedstring;
	}




}