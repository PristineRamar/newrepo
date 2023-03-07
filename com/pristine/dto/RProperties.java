package com.pristine.dto;

import java.io.File;
import com.pristine.util.PropertyManager;

public class RProperties {
	public String rootDir;
	public String inputDir;
	public String outputDir;
	private static RProperties rprops;
	private static String TOMCAT_PATH = (String)PropertyManager.getProperty("TOMCAT.PATH");
	private RProperties(){
		
	}
	public static RProperties getInstance(){
		if( rprops == null){
			rprops = new RProperties();
			rprops.rootDir = TOMCAT_PATH+File.separator +"webapps"+File.separator+"R-Project";
			rprops.inputDir = PropertyManager.getProperty("R.INPUTDIR");
			rprops.outputDir = PropertyManager.getProperty("R.OUTPUTDIR");
		}
		return rprops;
	}

}
