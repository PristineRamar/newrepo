package com.pristine.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dataload.prestoload.PriceAndCostLoader;
import com.pristine.dto.fileformatter.AholdMovementFile;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;

public class MoveFiles {

	private static Logger logger = Logger.getLogger("MoveFiles");
	private static String rootPath;
	
	private static String ARG_INP_REL_PATH = "INPUT_RELATIVE_PATH=";
	private static String ARG_OUT_REL_PATH = "OUTPUT_RELATIVE_PATH=";
	
	
	public MoveFiles() {
		PropertyManager.initialize("analysis.properties");		
	}
	
	/** Moves the files from source to destination
	 * @param args
	 */
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j.properties");
		
		MoveFiles moveFiles = new MoveFiles();

		if (args.length < 2) {
			logger.info("Minimum Arguments Missing");
			logger.info("Possible Arguments: INPUT_RELATIVE_PATH=[InputRelativePath] OUTPUT_RELATIVE_PATH=[OutputRelativePath]");
			System.exit(-1);
		}

		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");		 
		String source = rootPath + "/" + args[0].substring(ARG_INP_REL_PATH.length());
		String destination = rootPath + "/" + args[0].substring(ARG_INP_REL_PATH.length()) + "/" + args[1].substring(ARG_OUT_REL_PATH.length());
		ArrayList<String> fileList = null;
		try {
			fileList = moveFiles.getFiles(source);
		} catch (GeneralException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		PrestoUtil.moveFiles(fileList, destination);
	}

	private  ArrayList<String> getFiles(String specificPath) throws GeneralException 
	{
		String fullPath = specificPath;	    
	    ArrayList <String> fileList = new ArrayList <String> ();
	    File dir = new File(fullPath);	    
	   
	    String[] children = dir.list();
        if (children != null) {
            for (int i=0; i<children.length; i++) {
                // Get filename of file or directory
                String filename = children[i];               
                if( filename.toLowerCase().contains(".txt") || filename.toLowerCase().contains(".csv"))
                        fileList.add(fullPath + "/" +filename);
            }
        }
		return fileList;
	} 
}
