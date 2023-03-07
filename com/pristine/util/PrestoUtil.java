
package com.pristine.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import com.pristine.exception.GeneralException;
import com.pristine.util.offermgmt.PRCommonUtil;

public class PrestoUtil
{
	private static Logger	logger						= Logger.getLogger(PrestoUtil.class);
	public static double avg(int[] values)
	{
		int sum = 0, i;
		int l = values.length;
		for (i = 0; i < l; i++)
		{
			sum += values[i];
		}
		double average = (double) sum / (double) l;
		return average;
	}

	public static double avg(float[] values)
	{
		int i;
		double sum = 0;
		int l = values.length;
		for (i = 0; i < l; i++)
		{
			sum += values[i];
		}
		double average = sum / l;
		return average;
	}

	public static float limit(float value, String property)
	{
		float limitValue = Float.parseFloat(PropertyManager.getProperty(property));
		return value + ((limitValue * value) / 100);
	}

	public static float round(float Rval, int Rpl)
	{
		float p = (float) Math.pow(10, Rpl);
		Rval = Rval * p;
		float tmp = Math.round(Rval);
		return tmp / p;
	}

	public static void copy(String fromFileName, String toFileName) throws GeneralException, IOException
	{
		File fromFile = new File(fromFileName);
		File toFile = new File(toFileName);
		
		logger.info("copy-->FromFile>>"+fromFileName +">>>ToFile>>"+toFile);
		
		if (!fromFile.exists())
			throw new GeneralException("FileCopy: " + "no such source file: " + fromFileName);
		if (!fromFile.isFile())
			throw new GeneralException("FileCopy: " + "can't copy directory: " + fromFileName);
		if (!fromFile.canRead())
			throw new GeneralException("FileCopy: " + "source file is unreadable: " + fromFileName);

		if (toFile.isDirectory())
			toFile = new File(toFile, fromFile.getName());
		
		if (!toFile.createNewFile())
		{
			toFile.delete();
			toFile.createNewFile();
		}		

		FileInputStream from = null;
		FileOutputStream to = null;
		try
		{
			logger.info("###copy--> Started Copying file@@@@");
			from = new FileInputStream(fromFile);
			to = new FileOutputStream(toFile);
			byte[] buffer = new byte[4096];
			int bytesRead;

			while ((bytesRead = from.read(buffer)) != -1)
				to.write(buffer, 0, bytesRead); // write
			logger.info("###copy--> Completed Copying file@@@@");
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (from != null)
				try
				{
					from.close();
				}
				catch (IOException e)
				{
					;
				}
			if (to != null)
				try
				{
					to.close();
				}
				catch (IOException e)
				{
					;
				}
		}
	}
	
	public static ArrayList<String> csvStrToStrArray(String aString){
	    ArrayList<String> al = new ArrayList<String>();
	    if( aString != null ){
		    StringTokenizer st = new StringTokenizer(aString, ",");
		    while (st.hasMoreTokens()) {
		        al.add(st.nextToken());
		    }
		}
	    return al;
	}
	
	public static int getIntValue(CachedRowSet crs, String colName) throws SQLException {
		String valStr = crs.getString(colName);
		int retVal = -1;
		if ( valStr!= null && !valStr.equals(""))
			retVal = Integer.parseInt(valStr);
		return retVal;
	}
	
	public static void moveFiles (ArrayList <String> fileList, String destinationDirectory ) {
		for ( String sourceFile : fileList ) {
			moveFile (sourceFile, destinationDirectory);			
		}
	}
		
	public static void moveFile (String sourceFile, String destinationDirectory ) {
		
		File file = new File(sourceFile);		
		String fileName = file.getName();
		
		// Destination directory
		File dir = new File(destinationDirectory + "/" + file.getName() + "-" + DateUtil.now());

		boolean success = file.renameTo(dir);
		
		if (success) 
			logger.info("moveFile() - File moved.  Filename=" + fileName + ", destinationDirectory=" + destinationDirectory);		
		else		
			logger.error("moveFile() - File moving FAILED!  Filename=" + sourceFile + ", destinationDirectory=" + destinationDirectory);
		
	}
	
	public static String castUPC(String upc, boolean elevenDigitUPC) {
		String retUPC = upc;
		if( upc.length() > 11){
			
			boolean zeroPadded = true;
			for (int i = 0; i < upc.length()-11; i++ ){
				if( upc.charAt(i) != '0'){
					zeroPadded = false;
					break;
				}
			}
			if ( zeroPadded){
				if( elevenDigitUPC)
					retUPC = upc.substring(upc.length()-11);
				else
					retUPC = upc.substring(upc.length()-12);
			}
		}else{

			int padLength = 0;
			if( elevenDigitUPC)
				padLength = 11 - upc.length();
			else
				padLength = 12 - upc.length();
			
			StringBuffer upcPad = new StringBuffer (); 
			for ( int i = 0; i < padLength; i++)
				upcPad.append('0');
			retUPC = upcPad.toString() + upc;
		}
		return retUPC;
	}

	public static double getCost (double listCost, double dealCost)
	{
		return dealCost > 0 ? dealCost : listCost; 
	}
	
	
	
	public static void unzip(String zipFile, String destinationFolder) throws GeneralException{ 
	final int BUFFER = 2048;
		try {
			BufferedOutputStream dest = null;
			FileInputStream fis = new FileInputStream(zipFile);
			ZipInputStream zis = new ZipInputStream(
					new BufferedInputStream(fis));
			ZipEntry entry;
			
			while ((entry = zis.getNextEntry()) != null) {
				String zipFileEntry = entry.getName().trim();
				System.out.println("Extracting: " + zipFileEntry);
				int count;
				byte data[] = new byte[BUFFER];
				
				// write the files to the disk
				FileOutputStream fos = new FileOutputStream( destinationFolder + "/" + zipFileEntry);
				dest = new BufferedOutputStream(fos, BUFFER);
				
				// ??? exception occurs in next line when probably the zip file is not uploaded completely or ready to be read.
				while ((count = zis.read(data, 0, BUFFER)) != -1) {
					dest.write(data, 0, count);
				}
				dest.flush();
				dest.close();
			}
			zis.close();
			fis.close();
			
			logger.info("unzip() - Unzip completed.  Filename=" + zipFile);
		} catch (Exception e) {
			//e.printStackTrace();
			logger.error("unzip() ExceptionMessage=" + e.getMessage());
			logger.info("unzip() - Unzip FAILED.  Filename=" + zipFile);
			
			throw new GeneralException("unzip() - Problem with Unzipping!  Exception has occurred.  ", e);
		}
	}

	public static void deleteFiles(ArrayList<String> fileList) {
		
		for ( String sourceFile : fileList ) {
			
			File deleteFile = new File(sourceFile);
			String fileName = deleteFile.getName();
			
			boolean success = deleteFile.delete();
			
			if (success)
				logger.info("deleteFiles() - File deleted.  Filename=" + fileName);
			else
				logger.error("deleteFiles() - File deletion FAILED!  Filename=" + sourceFile);
		}
		
	}
	
	public static void deleteFile(String sourceFile) {
		File deleteFile = new File(sourceFile);
		String fileName = deleteFile.getName();
		boolean success = deleteFile.delete();
		if (success)
			logger.info("deleteFiles() - File deleted.  Filename=" + fileName);
		else
			logger.error("deleteFiles() - File deletion FAILED!  Filename=" + sourceFile);
	}

	/**
	 * This method compares two dates
	 * @param date1		Date to compare
	 * @param date2		Date to compare
	 * @return
	 */
    public static long compareTo( java.util.Date date1, java.util.Date date2 )  
    {  
    	//returns negative value if date1 is before date2  
    	//returns 0 if dates are even  
    	//returns positive value if date1 is after date2  
    	return date1.getTime() - date2.getTime();  
   }
    
   /**
    * This method pads store number according to the size of the store number specified in the properties file
    * @param storeNumber Input Store Number
    * @return	Store Number padded with appropriate 0's
    */
   public static String castStoreNumber(String storeNumber) {
		String retStoreNumber = storeNumber;
		int padLength = 0;
		
		padLength = Integer.parseInt(PropertyManager.getProperty("STORE_NUMBER_LENGTH","0")) - storeNumber.length();
			
		StringBuffer strNoPad = new StringBuffer (); 
		for ( int i = 0; i < padLength; i++)
			strNoPad.append('0');
		retStoreNumber = strNoPad.toString() + storeNumber;
		
		return retStoreNumber;
   }
   
   /**
    * This method rounds a decimal value to specified number of decimals
    * @param Rval	Value to be rounded off
    * @param Rpl	Number of decimals
    * @return		Rounded off value
    */
   public static double round(double Rval, int Rpl)
	{
	    double p = (float) Math.pow(10, Rpl);
		Rval = Rval * p;
		double tmp = Math.round(Rval);
		return tmp / p;
	}
   
   /**
    * This method pads zone number according to the size of the zone number specified in the properties file
    * @param zoneNumber Input Zone Number
    * @return	Store Number padded with appropriate 0's
    */
   public static String castZoneNumber(String zoneNumber) {
		String retZoneNumber = zoneNumber;
		int padLength = 0;
		
		padLength = Integer.parseInt(PropertyManager.getProperty("ZONE_NUMBER_LENGTH")) - zoneNumber.length();
			
		StringBuffer strNoPad = new StringBuffer (); 
		for ( int i = 0; i < padLength; i++)
			strNoPad.append('0');
		retZoneNumber = strNoPad.toString() + zoneNumber;
		
		return retZoneNumber;
   }
   
   /***
    * Get all the zip files from a folder
    * @param zipFilePath
    * @return
    * @throws GeneralException
    */
	public static ArrayList<String> getAllFilesInADirectory(String directoryPath, String fileExtension) throws GeneralException {
		ArrayList<String> fileList = new ArrayList<String>();
		File dir = new File(directoryPath);
		String[] children = dir.list();
		if (children != null) {
			for (int i = 0; i < children.length; i++) {
				// Get filename of file or directory
				String filename = children[i];

				// logger.debug(filename);
				if (filename.contains("."+ fileExtension)) {
					fileList.add(directoryPath + "/" + filename);
					logger.info("getZipFiles() - filename=" + filename);
				}
			}
		}
		return fileList;
	}

	public static void createDirIfNotExists(String destinationFolder) {
		File destFolder = new File(destinationFolder);
		if (!destFolder.isDirectory()) {
			new File(destinationFolder).mkdirs();
		}
	}
	
	public static void unzipIncludingSubDirectories(String inputZipFile, String destinationFolder) throws GeneralException {
		Charset CP866 = Charset.forName("CP866");
		ZipFile zipFile = null;
		try {
			zipFile = new ZipFile(inputZipFile, CP866);
            Enumeration e = zipFile.entries();
			while (e.hasMoreElements()) {
				ZipEntry entry = (ZipEntry) e.nextElement();
				File destinationFilePath = new File(destinationFolder, entry.getName());

				// create directories if required.
				destinationFilePath.getParentFile().mkdirs();

				// if the entry is directory, leave it. Otherwise extract it.
				if (entry.isDirectory()) {
					continue;
				} else {
					System.out.println("Extracting " + destinationFilePath);

					/*
					 * Get the InputStream for current entry of the zip file
					 * using
					 *
					 * InputStream getInputStream(Entry entry) method.
					 */
					BufferedInputStream bis = new BufferedInputStream(zipFile.getInputStream(entry));

					int b;
					byte buffer[] = new byte[1024];

					/*
					 * read the current entry from the zip file, extract it and
					 * write the extracted file.
					 */
					FileOutputStream fos = new FileOutputStream(destinationFilePath);
					BufferedOutputStream bos = new BufferedOutputStream(fos, 1024);

					while ((b = bis.read(buffer, 0, 1024)) != -1) {
						bos.write(buffer, 0, b);
					}

					// flush the output stream and close it.
					bos.flush();
					bos.close();

					// close the input stream.
					bis.close();
				}
			}
		} catch (Exception e) {
			// e.printStackTrace();
			logger.error("unzip() ExceptionMessage=" + e.getMessage());
			logger.info("unzip() - Unzip FAILED.  Filename=" + inputZipFile);
			throw new GeneralException("unzip() - Problem with Unzipping!  Exception has occurred.  ", e);
		} finally {
			try {
				zipFile.close();
			} catch (IOException e) {
				throw new GeneralException("Error while closing zipFile.  ", e);
			}
		}
	}
	
	public static boolean checkIfFileIsPresentInAZipFile(String zipFilePath, String fileKey) throws GeneralException {
		Charset CP866 = Charset.forName("CP866");
		ZipFile zipFile = null;
		boolean isFilePresent = false;
		try {
			zipFile = new ZipFile(zipFilePath, CP866);
			Enumeration e = zipFile.entries();
			while (e.hasMoreElements()) {
				ZipEntry entry = (ZipEntry) e.nextElement();
				if (entry.getName().toLowerCase().contains(fileKey)) {
					isFilePresent = true;
					break;
				}
			}
		} catch (Exception e) {
			logger.error("unzip() ExceptionMessage=" + e.getMessage());
			throw new GeneralException("unzip() - Problem with Unzipping!  Exception has occurred.  ", e);
		} finally {
			try {
				zipFile.close();
			} catch (IOException e) {
				throw new GeneralException("Error while closing zipFile.  ", e);
			}
		}
		return isFilePresent;
	}
	
	/**
	 * This method zip's the given directory
	 * @param dir
	 * @param zipDirName
	 */
    public static void zipDirectory(File dir, String zipDirName) {
        try {
        	List<String> filesListInDir = new ArrayList<String>();
            populateFilesList(dir, filesListInDir);
            FileOutputStream fos = new FileOutputStream(zipDirName);
            ZipOutputStream zos = new ZipOutputStream(fos);
            for(String filePath : filesListInDir){
                System.out.println("Zipping "+filePath);
                //for ZipEntry we need to keep only relative file path, so we used substring on absolute path
                ZipEntry ze = new ZipEntry(filePath.substring(dir.getAbsolutePath().length()+1, filePath.length()));
                zos.putNextEntry(ze);
                //read the file and write to ZipOutputStream
                FileInputStream fis = new FileInputStream(filePath);
                byte[] buffer = new byte[1024];
                int len;
                while ((len = fis.read(buffer)) > 0) {
                    zos.write(buffer, 0, len);
                }
                zos.closeEntry();
                fis.close();
            }
            zos.close();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	/**
	 * This method zip's the given directory
	 * @param dir
	 * @param inputFilePath
	 */
    public static void zipFile(String inputFileName, String inputFilePath, String outputZip) {
        try {
            FileOutputStream fos = new FileOutputStream(inputFilePath + "/" + outputZip);
            ZipOutputStream zos = new ZipOutputStream(fos);

                System.out.println("Zipping "+ inputFileName);
                //for ZipEntry we need to keep only relative file path, so we used substring on absolute path
                ZipEntry ze = new ZipEntry(inputFileName);
                zos.putNextEntry(ze);
                //read the file and write to ZipOutputStream
                FileInputStream fis = new FileInputStream(inputFilePath + "/" + inputFileName);
                byte[] buffer = new byte[1024];
                int len;
                while ((len = fis.read(buffer)) > 0) {
                    zos.write(buffer, 0, len);
                }
                zos.closeEntry();
                fis.close();
                zos.close();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }    
    
    
    /**
     * This method populates all the files in a directory to a List
     * @param dir
     * @throws IOException
     */
    private static void populateFilesList(File dir, List<String> filesListInDir) throws IOException {
        File[] files = dir.listFiles();
        if(files != null){
        for(File file : files){
            if(file.isFile()){
            	filesListInDir.add(file.getAbsolutePath());
            }
            else{
            	populateFilesList(file, filesListInDir);
            }
        }
        }
    }
    
    /**
     * To get path of current program
     * @return
     */
    public static String getPath(){
    	return Paths.get(".").toAbsolutePath().normalize().toString();
    }
    
	public static String getTimeTakenInMins(long startTime, long endTime) {
		String timeTakenStr = "";
		long diffMilles = endTime - startTime;
		double timeInSecs = diffMilles / 1000;
		double timeInMins = timeInSecs / 60;
		timeTakenStr = PRCommonUtil.round(timeInMins, 2) + " mins";
		return timeTakenStr;
	}
	
	public static List<String> getFiles(String dir) {
    	logger.info("getFiles()- Directory: " + dir);
        return Stream.of(new File(dir).listFiles())
          .filter(file -> !file.isDirectory())
          .map(File::getName)
          .collect(Collectors.toList());
    }
	
	public double calculateAverage(List<Double> marks) {
		  Double sum = 0.0;
		  if(!marks.isEmpty()) {
		    for (Double mark : marks) {
		        sum += mark;
		    }
		    
		    return sum / marks.size();
		  }
		  return sum;
		}

	public static <K, V> K getKey(Map<K, V> map, V value) {
		for (K key : map.keySet()) {
			if (value.equals(map.get(key))) {
				return key;
			}
		}
		return null;
	}
	
}
