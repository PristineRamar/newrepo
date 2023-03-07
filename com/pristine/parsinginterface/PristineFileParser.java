/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pristine.parsinginterface;

import com.csvreader.CsvReader;
import com.pristine.dao.DBManager;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.sun.xml.internal.bind.v2.runtime.reflect.opt.Const;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Time;
import java.util.List;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Date;

import org.apache.log4j.Logger;

/**
 *
 * @author sakthidasan
 */
public abstract class PristineFileParser<T> {

    Connection oracleConnection = null;
    String rootPath = null;
    int commitRecCount = 1000;
    private Logger logger = Logger.getLogger("DataParser");
    protected boolean headerPresent = false;

    /**
	 * This class or its subclasses are used by programs that may read in 
	 * a properties file at the beginning of their execution. This constructor 
	 * may be used in such scenarios.
	 * @param initProps to be set
	 */
	public PristineFileParser(boolean initProps) {
		if(initProps) {
			init ("analysis.properties");
		}
		else {
			logger.info("NOT reading any .properties file while initializing DataParser!");
			logger.debug("The child class instance or the class initializing the child class has guranteed that the necessary .property file will be read!");
		}
	}
    
    public PristineFileParser()
    {
    	init ("analysis.properties");
    }
    
    public PristineFileParser (String propFile)
    {
    	init (propFile);
    }
    
    public void init (String propFile)
    {
        try {
            
            PropertyManager.initialize(propFile);
            rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
            String tempCommitCount = PropertyManager.getProperty("DATALOAD.COMMITRECOUNT", "1000");
            commitRecCount=Integer.parseInt(tempCommitCount);
            
            oracleConnection = DBManager.getConnection();
        } catch (GeneralException ex) {
            logger.error("Error in File Parser Intialization", ex);
        }
    }
    
    public ArrayList<String> getZipFiles(String specificPath) throws GeneralException 
	{
    	String fullPath = rootPath;
	    if(specificPath!=null && specificPath.trim().length()>0)
	    {
	        fullPath=fullPath + "/"+specificPath;
	    }
	    ArrayList <String> fileList = new ArrayList <String> ();

	    File dir = new File(fullPath);
	    String[] children = dir.list();
        if (children != null) {
            for (int i=0; i<children.length; i++) {
                // Get filename of file or directory
                String filename = children[i];
                
                //logger.debug(filename);
                if( filename.contains(".zip"))
                {
                     fileList.add(fullPath + "/" + filename);
                     logger.info("getZipFiles() - filename=" + filename);
                }
            }
        }
		return fileList;
	}
    
	public ArrayList<String> getFiles(String specificPath) throws GeneralException 
	{
		String fullPath = rootPath;
	    if(specificPath!=null && specificPath.trim().length()>0)
	    {
	    	fullPath=fullPath+"/"+specificPath;
	    }
	    
	    ArrayList <String> fileList = new ArrayList <String> ();

	    File dir = new File(fullPath);
	    
	    /*
	    File dir = new File(fullPath);
	    
	    
	    FilenameFilter filter = new FilenameFilter() {
	        public boolean accept(File dir, String name) {
	            return name.contains(".zip");
	        }
	    };

	    String[] children = dir.list(filter); //looking for Zip files only 
        if (children != null) {
            for (int i=0; i<children.length; i++) {
            	String filename = fullPath + "/" + children[i];
            	//unzip the file
            	PrestoUtil.unzip(filename) ;
            }
        }
	    
	    String[] children = dir.list();
        if (children != null) {
            for (int i=0; i<children.length; i++) {
                // Get filename of file or directory
                String filename = children[i];
                logger.debug(filename);
                if( filename.toLowerCase().contains(".txt") || filename.toLowerCase().contains(".csv"))
                {
                	fileList.add(fullPath + "/" +filename);
                	logger.info("getFiles() - filename=" + filename);
                }
                else
                {
                	logger.info("getFiles() - filename=" + filename);
                }
            }
        }
        
	    */

        
        File[] files = dir.listFiles();
        
        for (int i = 0; i < files.length; i++) {
        	if (files[i].isFile()) {
        		String filename = files[i].getName();
                
        		if ( filename.toLowerCase().contains(".txt") || filename.toLowerCase().contains(".csv") || filename.toLowerCase().contains(".xls") )
                {
                	fileList.add(fullPath + "/" +filename);
            		logger.info("getFiles() - Valid data file found. name=" + filename + ", Bytes=" + files[i].length() + ", canRead=" + files[i].canRead() + ", lastModified=" + new Date(files[i].lastModified()));
                }
                else if ( filename.toLowerCase().contains(".zip"))
                {
            		logger.info("getFiles() - Zip file found.        name=" + filename + ", Bytes=" + files[i].length() + ", canRead=" + files[i].canRead() + ", lastModified=" + new Date(files[i].lastModified()));
                }
                else 
                {
            		logger.warn("getFiles() - INVALID file found.    name=" + filename + ", Bytes=" + files[i].length() + ", canRead=" + files[i].canRead() + ", lastModified=" + new Date(files[i].lastModified()));
                }
        	}
        }
        
		return fileList;
	}
	
    public Connection getOracleConnection() {
        return oracleConnection;
    }

    public String getRootPath() {
        return rootPath;
    }
    
    public void setHeaderPresent(boolean value) {
        headerPresent = value;
    }

    public List<T> parseDelimitedFile(Class returnObject, String fileName, char delimiter, String[] fieldNames,int limit)
    throws GeneralException
    {
        boolean fieldAuto = false;
        List<T> listobj = new ArrayList<T>();
        CsvReader reader = null;
        try {

            Object[] fieldArray = null;
            if (fieldNames == null) {
                fieldArray = returnObject.getDeclaredFields();
                fieldAuto = true;
            } else {
                fieldArray = fieldNames;
            }
            reader = readFile(fileName, delimiter);
            String line[];
            int limitcount=0;
            while (reader.readRecord()) {
            	//Skip header Record.
            	if( headerPresent && limitcount == 0){ 
            		limitcount++;
            		continue;
            	}
                Object objDTO = returnObject.newInstance();
                line = reader.getValues();
                if (line.length >= fieldArray.length) {
                    for (int i = 0; i < fieldArray.length; i++) {

                        try {
                            Field field = null;
                            if (fieldAuto) {
                                field = (Field) fieldArray[i];
                            } else {
                                field = returnObject.getDeclaredField(fieldArray[i].toString().replace(" ", ""));
                            }
                            field.setAccessible(true);
                            field=setFieldValue(field, objDTO, line[i].replace(",", "") );
                        } catch (Exception e) {
                            e.printStackTrace();
                        } catch (GeneralException e) {
                            e.printStackTrace();
                        }

                    }
                    listobj.add((T) objDTO);
                }
                if(limit!=0 && limit==limitcount)
                {
                    break;
                }
                limitcount++;
                
                if( limitcount > 0 && (limitcount%commitRecCount == 0)){
                	processRecords(listobj);
                	listobj.clear();
                }

            }
            processRecords(listobj); //This is to process remainder items
        } catch (Exception e) {
        	throw new GeneralException( "File read error ", e);
        } finally{
        	if( reader != null){
        		reader.close();
        	}
        }
        
        return (List<T>) listobj;
    }
    
    
	public List<T> parseDelimitedFile(Class returnObject, String fileName, char delimiter, String[] fieldNames,
			int limit, String encoding) throws GeneralException {
		boolean fieldAuto = false;
		List<T> listobj = new ArrayList<T>();
		CsvReader reader = null;
		try {

			Object[] fieldArray = null;
			if (fieldNames == null) {
				fieldArray = returnObject.getDeclaredFields();
				fieldAuto = true;
			} else {
				fieldArray = fieldNames;
			}
			reader = readFile(fileName, delimiter, encoding);
			String line[];
			int limitcount = 0;
			while (reader.readRecord()) {
				// Skip header Record.
				if (headerPresent && limitcount == 0) {
					limitcount++;
					continue;
				}
				Object objDTO = returnObject.newInstance();
				line = reader.getValues();
				if (line.length >= fieldArray.length) {
					for (int i = 0; i < fieldArray.length; i++) {

						try {
							Field field = null;
							if (fieldAuto) {
								field = (Field) fieldArray[i];
							} else {
								field = returnObject.getDeclaredField(fieldArray[i].toString().replace(" ", ""));
							}
							field.setAccessible(true);
							field = setFieldValue(field, objDTO, line[i].replace(",", ""));
						} catch (Exception e) {
							e.printStackTrace();
						} catch (GeneralException e) {
							e.printStackTrace();
						}

					}
					listobj.add((T) objDTO);
				}
				if (limit != 0 && limit == limitcount) {
					break;
				}
				limitcount++;

				if (limitcount > 0 && (limitcount % commitRecCount == 0)) {
					processRecords(listobj);
					listobj.clear();
				}

			}
			processRecords(listobj); // This is to process remainder items
		} catch (Exception e) {
			throw new GeneralException("File read error ", e);
		} finally {
			if (reader != null) {
				reader.close();
			}
		}

		return (List<T>) listobj;
	}

    abstract public void processRecords(List<T> listobj) throws GeneralException;
    
    protected Field setFieldValue(Field field, Object dtoObject, String value) 
    throws GeneralException {
        try {
            if (field.getType() == String.class) {
                field.set(dtoObject, value);
            } else if (field.getType() == float.class) {
            	if( value!= null && !value.equals(""))
            		field.setFloat(dtoObject, Float.parseFloat(value.replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY)));
            	else
            		field.setFloat(dtoObject, 0);
            	
            } else if (field.getType() == double.class) {
            	if( value!= null && !value.equals(""))
            		field.setDouble(dtoObject, Double.parseDouble(value.replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY)));
            	else
            		field.setDouble(dtoObject, 0);
            		
            }
            else if (field.getType() == int.class) {
            	if(value != null) {
            		String valReplaced = value.replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY)
            				.replaceAll(Constants.DECIMAL, Constants.EMPTY);
                	if(!valReplaced.equals("")) {
                		
                		field.setInt(dtoObject, Integer.parseInt(valReplaced));
                	}	
            	}
            } else if (field.getType() == boolean.class) {
                field.setBoolean(dtoObject, Boolean.parseBoolean(value));
            }
        } catch (Exception e) {
        	throw new GeneralException( "File Parser - Setting Values", e);
        }
        return field;
    }

    public List<T> parseTextFile(Class returnObject, String fileName, LinkedHashMap fieldNameCollection, int limit) 
    	throws GeneralException
    {
        List<T> listobj = new ArrayList<T>();
        CsvReader reader = null;
        try {
            reader = readFile(fileName, '0');
            String line;
            int limitcount=0;
            while (reader.readRecord()) {
                Object objDTO = returnObject.newInstance();
                line = reader.getRawRecord();
                Iterator keyIterator=fieldNameCollection.keySet().iterator();
                
                    while(keyIterator.hasNext()) {
                        String fieldName=keyIterator.next().toString();
                        try {
                               Field field = returnObject.getDeclaredField(fieldName);
                            field.setAccessible(true);
                            String value=(String) fieldNameCollection.get(fieldName);
                            String[] indexArray=value.split(Constants.INDEX_DELIMITER);
                            if(indexArray.length==2)
                            {
                             int startPosition=Integer.parseInt(indexArray[0]);
                             int endPosition=Integer.parseInt(indexArray[1]);
                            field=setFieldValue(field, objDTO, line.substring(startPosition, endPosition));
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    listobj.add((T) objDTO);
                
                limitcount++;

                if(limit >0 && limit==limitcount)
                {
                    break;
                }
                if( limitcount > 0 && (limitcount%commitRecCount == 0)){
                	processRecords(listobj);
                	listobj.clear();
                }
            }
            processRecords(listobj); //This is to process remainder items
           
        } catch (Exception e) {
        	throw new GeneralException( "Error in File parsing ", e);
        }finally{
        	if( reader != null){
        		reader.close();
        	}
        }

        return (List<T>) listobj;
    }

    protected CsvReader readFile(String fileName, char delimiter) throws GeneralException{
        CsvReader reader = null;
        try {
            reader = new CsvReader(new FileReader(fileName));
            if (delimiter != '0') {
                reader.setDelimiter(delimiter);
            }
        } catch (Exception e) {
        	throw new GeneralException( "File read error ", e);
        }
        return reader;
    }
    
    protected CsvReader readFile(String fileName, char delimiter, String encoding) throws GeneralException{
        CsvReader reader = null;
        try {
        	BufferedReader in = new BufferedReader(
        		       new InputStreamReader(
        		                  new FileInputStream(fileName), encoding));
            reader = new CsvReader(in);
            if (delimiter != '0') {
                reader.setDelimiter(delimiter);
            }
        } catch (Exception e) {
        	throw new GeneralException( "File read error ", e);
        }
        return reader;
    }
    
    public void performCloseOperation(Connection conn, boolean commit){
	try{
			if( PropertyManager.getProperty("DATALOAD.COMMIT", "").equalsIgnoreCase("TRUE") && commit){
				logger.info("Committing transacation");
				PristineDBUtil.commitTransaction(conn, "Data Load");
			}
			else{
				logger.info("Rolling back transacation");
				PristineDBUtil.rollbackTransaction(conn, "Data Load");
			}
		}catch(GeneralException ge){
			logger.error("Error in commit", ge);
			System.exit(1);
		}finally{
			PristineDBUtil.close(conn);
		}
    }
    
    public String trim(String str){
    	if( str != null)
    		return str.trim();
    	return str;
    }
    
	public List<T> parseDelimitedFileV2(Class returnObject, String fileName, char delimiter, String[] fieldNames,
			int limit, String encoding) throws GeneralException {
		boolean fieldAuto = false;
		List<T> listobj = new ArrayList<T>();
		BufferedReader reader = null;
		try {

			Object[] fieldArray = null;
			if (fieldNames == null) {
				fieldArray = returnObject.getDeclaredFields();
				fieldAuto = true;
			} else {
				fieldArray = fieldNames;
			}
			reader = readFileUsingBufferedReader(fileName, delimiter, encoding);
			// String line[];
			int limitcount = 0;

			String record = reader.readLine();
			while (record != null) {
				// replace qoutes
				record = record.replace("\"", "");
				record = record.replace("'", "");
				// split line according to given delimiter
				String[] line = record.split("\\" + delimiter);
				// handle items...

				// Skip header Record.
				if (headerPresent && limitcount == 0) {
					limitcount++;
					continue;
				}
				Object objDTO = returnObject.newInstance();
				if (line.length >= fieldArray.length) {
					for (int i = 0; i < fieldArray.length; i++) {

						try {
							Field field = null;
							if (fieldAuto) {
								field = (Field) fieldArray[i];
							} else {
								field = returnObject.getDeclaredField(fieldArray[i].toString().replace(" ", ""));
							}
							field.setAccessible(true);
							field = setFieldValue(field, objDTO, line[i].replace(",", "").trim());
						} catch (Exception e) {
							e.printStackTrace();
						} catch (GeneralException e) {
							e.printStackTrace();
						}

					}
					listobj.add((T) objDTO);
				}
				if (limit != 0 && limit == limitcount) {
					break;
				}
				limitcount++;

				if (limitcount > 0 && (limitcount % commitRecCount == 0)) {
					processRecords(listobj);
					listobj.clear();
				}
				record = reader.readLine();
			}
			processRecords(listobj); // This is to process remainder items
		} catch (Exception e) {
			throw new GeneralException("File read error ", e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					throw new GeneralException("File read error ", e);
				}
			}
		}

		return (List<T>) listobj;
	}

	protected BufferedReader readFileUsingBufferedReader(String fileName, char delimiter, String encoding)
			throws GeneralException {
		BufferedReader reader = null;
		try {
			File file = new File(fileName);
			InputStream is = new FileInputStream(file);
			reader = new BufferedReader(new InputStreamReader(is, encoding));

		} catch (Exception e) {
			throw new GeneralException("File read error ", e);
		}
		return reader;
	}
}
