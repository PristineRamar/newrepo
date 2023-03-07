package com.pristine.util;

import java.io.File;
import java.io.FileInputStream;   
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;   
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.hssf.usermodel.HSSFCell;   
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRow;   
import org.apache.poi.hssf.usermodel.HSSFSheet;   
import org.apache.poi.hssf.usermodel.HSSFWorkbook;   
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Row;   
import org.apache.poi.ss.usermodel.Sheet;   
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.pristine.parsinginterface.ExcelFileParserV2;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.dto.ArrayListRows;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleItemDTO;
import com.pristine.dto.webservice.CompDataDTO;
import com.pristine.exception.GeneralException;


public class ExcelFileMerger  extends PristineFileParser
{   
	
	public static ArrayList<ArrayListRows> totalList = null;
	public ExcelFileParserV2 parser = null;
	static Logger logger = Logger.getLogger("ExcelFileMerger");
	static String rootPath = "";
	private FileWriter fw = null;
	private PrintWriter pw = null;
	private int recordCount = 0;
	
	public ExcelFileMerger(){
		super ("analysis.properties");
	}
	
	
  public static void main(String args[]) throws Exception   
  {   
	  ExcelFileMerger mergeExcel = new ExcelFileMerger();
		String outputFolder="";
		String subFolder = "";
		String store = "";
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		PropertyConfigurator.configure("log4j-merger-excel.properties");
		logCommand (args);
		for (int ii = 0; ii < args.length; ii++) {
			String arg = args[ii];	 
			if (arg.startsWith("SUBFOLDER")) {
				subFolder = arg.substring("SUBFOLDER=".length());
			}
			if(arg.startsWith("STORE")){
				store = arg.substring("STORE=".length());
			}
			if(arg.startsWith("OUTPUT")){
				outputFolder = arg.substring("OUTPUT=".length());
			}
			outputFolder = rootPath + "/" + subFolder + "/" + outputFolder;
		}
		
		try {
			mergeExcel.processMerging(subFolder,outputFolder,store);
		} catch (GeneralException e) {
			// TODO Auto-generated catch block
			logger.error("Error occured in file processing",e);
			e.printStackTrace();
		}	
  }
  private void mapFieldNames(TreeMap<Integer, String> fieldNames, String store) {
			fieldNames.put(0, "storeName");
			fieldNames.put(1, "compStrNo");
			fieldNames.put(2, "checkDate");
			fieldNames.put(3, "retailerItemCode");
			fieldNames.put(4, "itemName");
			fieldNames.put(5, "regMPack");
			fieldNames.put(6, "regPrice");
			fieldNames.put(7, "size");
			fieldNames.put(8, "upc");
			fieldNames.put(9, "outSideRangeInd");
			fieldNames.put(10, "saleMPack");
			fieldNames.put(11, "fSalePrice");
			fieldNames.put(12, "categoryName");
			
			if(store.equals("HN")){
				fieldNames.put(13, "plFlag");
			}else if(store.equals("WG")){
				fieldNames.put(13, "aisle");
				fieldNames.put(14,"plFlag");
			}
	}	
  
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public void processMerging(String subFolder, String path, String store) throws GeneralException, Exception{
			parser = new ExcelFileParserV2();
			Set<String> weekNoSet = new HashSet<>();
			TreeMap<Integer, String> fieldNames = new TreeMap<Integer, String>();
			mapFieldNames(fieldNames, store);
			ArrayList<String> fileList = parser.getFiles(subFolder);
			ArrayList<String> filesToMove = new ArrayList<String>();
			String filesPath = getRootPath() + "/" + subFolder;
			  totalList = new ArrayList<ArrayListRows>();
			  logger.info("Processing  : " + subFolder + " " + path + " " + store);
				   ExcelFileMerger mergeExcel = new ExcelFileMerger();   
				   ArrayListRows firstRowList = null;
				   String weekNo = "";
				   String storeNo = "";
				   List<CompDataDTO> compDataFinalList = new ArrayList<CompDataDTO>();
				   for(int i = 0;i < fileList.size(); i++){
					   String file = fileList.get(i);
					   String[] splitFD = file.split("/");
					   String fileName = splitFD[splitFD.length - 1];
					   //20738_Week2 Category_Sep 3, 2014 11_26_48 PM.xls
					   //50_Week2 Category_Sep 3, 2014 7_07_52 PM.xls
					   //C:\Users\8kmiles\Desktop\Price Check\SS\Wegmans\14_Week1 Category_Aug 28, 2014 12_10_50 PM.xls
					   String[] splitFN = fileName.split("_");
					   storeNo = splitFN[0];
					   weekNo = splitFN[1].replaceAll(" Category", "").trim();
					   if(weekNo.contains("Week")){
						   weekNoSet.add(weekNo);
					   }
					   int sheetNo = 0;
					   parser.setFirstRowToProcess(1);
					   List<CompDataDTO> compDataList = null;
					   if(storeNo.length() == 5 && store.equalsIgnoreCase("HN")){
						   filesToMove.add(file);
					   	  logger.info("Processing HANNAFORD file : " + file);
					   	  compDataList = parser.parseExcelFile(CompDataDTO.class, file, sheetNo, fieldNames);
//						  Workbook workBook1 = mergeExcel.checkFileFormat(fileList.get(i));   
//			        	  ArrayList<ArrayListRows> rowsArrayList = mergeExcel.getObjectsFromWorkBook(workBook1);     
//						  firstRowList = rowsArrayList.get(0);   
//						  rowsArrayList.remove(0);   
//						  totalList.addAll(rowsArrayList);
					   }
					   else if(storeNo.length() < 5 && store.equalsIgnoreCase("WG")){
						   filesToMove.add(file);
					   	   logger.info("Processing WEGMANS file :" + file);
					   	   compDataList = parser.parseExcelFile(CompDataDTO.class, file, sheetNo, fieldNames);
//						   Workbook workBook1 = mergeExcel.checkFileFormat(fileList.get(i));   
//						   ArrayList<ArrayListRows> rowsArrayList = mergeExcel.getObjectsFromWorkBook(workBook1);     
//						   firstRowList = rowsArrayList.get(0);   
//						   rowsArrayList.remove(0);   
//						   totalList.addAll(rowsArrayList);
					   }
					   if(compDataList!= null && compDataList.size()>0){
						   compDataFinalList.addAll(compDataList);
					   }
				   }
				   String weekNumber = "";
				   String weekAlpha = "";
				   for(String week: weekNoSet){
					   String tmp = week.replaceAll("Week", "").trim();
					   try{
						   int weekId = Integer.parseInt(tmp);
						   weekNumber = String.valueOf(weekId);
					   }catch(Exception e){
						   weekAlpha = tmp;
					   }
				   }
				   String fileName = "Week " + weekNumber + "-" + weekAlpha;
//				   	mergeExcel.writeToFile(path,totalList,firstRowList, fileName, store);
				   	writeFile(path, compDataFinalList, fileName, store);
				   	pw.flush();
					fw.flush();
					pw.close();
					fw.close();
					PrestoUtil.moveFiles(filesToMove, filesPath + "/" + Constants.COMPLETED_FOLDER);
		}
				  
		private void writeHeaderInfo(String store){
			pw.print("Store Name");
			pw.print("|");
			pw.print("Comp Store No"); 
			pw.print("|");
			pw.print("Check Date"); 
			pw.print("|");
			pw.print("Retailer Item code");
			pw.print("|");
			pw.print("Item Desc");
			pw.print("|");
			pw.print("Reg Quantity");
			pw.print("|");
			pw.print("Reg Price");
			pw.print("|");
			pw.print("Size"); 
			pw.print("|");
			pw.print("UPC");
			pw.print("|");
			pw.print("Outside Indicator");
			pw.print("|");
			pw.print("Sale Quantity");
			pw.print("|");
			pw.print("Sale Price");
			pw.print("|");
			pw.print("Category");
			pw.print("|");
			
			if(store.equals("HN")){
				pw.print("PL Flag");
				pw.println("       ");
			}else if(store.equals("WG")){
				pw.print("Aisle");
				pw.print("|");
				pw.print("PL Flag");
				pw.println("       ");
			}
		}
		
		private void writeFile(String path, List<CompDataDTO> itemList, String weekNo, String store) throws Exception {
			

		      String storeName = "";
		      if(store.equalsIgnoreCase("HN")){
		     	 storeName = "Hannaford";
		      }
		      else if(store.equalsIgnoreCase("WG")){
		     	 storeName = "Wegmans";
		      }
		    String outputPath = path + "/" + weekNo + "_" + storeName + ".txt";
		    
		  	File file = new File(outputPath);
			if (!file.exists())
				fw = new FileWriter(outputPath);
			else
				fw = new FileWriter(outputPath, true);

			pw = new PrintWriter(fw);
			
			writeHeaderInfo(store);
			
			for (int j = 0; j < itemList.size(); j++) {
				CompDataDTO competitiveDataDTO = itemList.get(j);
				if (competitiveDataDTO.upc != null && !competitiveDataDTO.upc.isEmpty()) {
					competitiveDataDTO.upc = Long.toString((long) (Double.valueOf(competitiveDataDTO.upc).longValue()));
					recordCount++;
					pw.print(competitiveDataDTO.storeName);
					pw.print("|");
					pw.print(competitiveDataDTO.compStrNo); 
					pw.print("|");
					pw.print(competitiveDataDTO.checkDate); 
					pw.print("|");
					pw.print("");
					pw.print("|");
					pw.print(competitiveDataDTO.itemName);
					pw.print("|");
					pw.print(competitiveDataDTO.regMPack);
					pw.print("|");
					pw.print(competitiveDataDTO.regPrice);
					pw.print("|");
					pw.print(competitiveDataDTO.size); 
					pw.print("|");
					pw.print(competitiveDataDTO.upc);
					pw.print("|");
					pw.print(competitiveDataDTO.outSideRangeInd);
					pw.print("|");
					pw.print(competitiveDataDTO.saleMPack);
					pw.print("|");
					pw.print(competitiveDataDTO.fSalePrice);
					pw.print("|");
					pw.print(competitiveDataDTO.categoryName);
					pw.print("|");
					
					if(store.equals("HN")){
						pw.print(competitiveDataDTO.plFlag);
						pw.println("       ");
					}else if(store.equals("WG")){
						pw.print(competitiveDataDTO.aisle);
						pw.print("|");
						pw.print(competitiveDataDTO.plFlag);
						pw.println("       ");
					}
				}
			}
			
			logger.info("Output file..." +path + "/" + weekNo + "_" + storeName + ".txt");
         	logger.info(path + "/" + weekNo + "_" + storeName + ".txt " + "created successfully...");
		}	
		
public int checkFileFormat;   
  public void writeToFile(String path,ArrayList<ArrayListRows> totalList,ArrayListRows firstRowList1, String weekNo, String store) throws Exception   
    {   

	      String storeName = "";
	      if(store.equalsIgnoreCase("HN")){
	     	 storeName = "Hannaford";
	      }
	      else if(store.equalsIgnoreCase("WG")){
	     	 storeName = "Wegmans";
	      }
	    String outputPath = path + "/" + weekNo + "_" + storeName + ".txt";
	  	File file = new File(outputPath);
		if (!file.exists())
			fw = new FileWriter(outputPath);
		else
			fw = new FileWriter(outputPath, true);

		pw = new PrintWriter(fw);

	  
	  	/*XSSFWorkbook xssfwb = new XSSFWorkbook();
	  	SXSSFWorkbook wb1 = new SXSSFWorkbook(xssfwb, 100);
        Sheet sheet1 = wb1.createSheet("new sheet");   
    	for(int k=0;k<14;k++)   
    	{   
    		sheet1.setColumnWidth( k, (short) 5000);   
    	}   
            CellStyle style = wb1.createCellStyle();
            style.setBorderBottom((short) 1);
            style.setBorderLeft((short) 1);
            style.setBorderRight((short) 1);
            style.setBorderTop((short) 1);
            style.setBottomBorderColor(HSSFColor.AQUA.index);
            style.setTopBorderColor(HSSFColor.AQUA.index);
            style.setRightBorderColor(HSSFColor.AQUA.index);
            style.setLeftBorderColor(HSSFColor.AQUA.index);
            style.setFillForegroundColor(HSSFColor.DARK_BLUE.index);
            style.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
            style.setAlignment(HSSFCellStyle.ALIGN_CENTER_SELECTION);
            Row newTitleRow = sheet1.createRow((short)0);
            Font fo = wb1.createFont();
            fo.setFontName("Arial");
            newTitleRow.setHeight((short) 500);
            fo.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);
            fo.setColor(HSSFColor.WHITE.index);
            style.setFont(fo);*/
        	   
                     
             for(int index=0;index<firstRowList1.coloumns.size();index++)   
                  {   
                   	//writeToCellXL(newTitleRow, index, firstRowList1.coloumns.get(index), style);
            	 		pw.print(firstRowList1.coloumns.get(index));
     					if(index != (firstRowList1.coloumns.size() -1))
     						pw.print("|");
                  }         
             pw.println("");
            /* CellStyle style1 = wb1.createCellStyle();
             style1.setBorderBottom((short) 1);
             style1.setBorderLeft((short) 1);
             style1.setBorderRight((short) 1);
             style1.setBorderTop((short) 1);
             style1.setBottomBorderColor(HSSFColor.AQUA.index);
             style1.setTopBorderColor(HSSFColor.AQUA.index);
             style1.setRightBorderColor(HSSFColor.AQUA.index);
             style1.setLeftBorderColor(HSSFColor.AQUA.index);
             Font fo1 = wb1.createFont();
             fo1.setFontName("Arial");
             style1.setFont(fo1);*/
             try{
            	 for(int index=0;index<totalList.size();index++)   
                 {   
                       //Row newRow = sheet1.createRow(index + 1);   
                       ArrayListRows temp = totalList.get(index);   
                       for(int index1=0;index1<temp.coloumns.size();index1++)   
                          {   
                           	//writeToCellXL(newRow, index1, temp.coloumns.get(index1), style1);
                    	   pw.print(temp.coloumns.get(index1));
        					if(index1 != (temp.coloumns.size() -1))
        						pw.print("|");
                          }
                       pw.println("");
                       if(index % 1000 == 0)
                       logger.info("processed :" + index);
                 } 

                 ExcelFileMerger.totalList.clear();
                 totalList.clear();
             } catch (Exception e){
             	logger.error("Error while writing file" + e);
             }
            
             
             	logger.info("Output file..." +path + "/" + weekNo + "_" + storeName + ".txt");
             	/*File f1 = new File(path + "/" + weekNo + "_" + storeName + ".csv");
                FileOutputStream fileOut = new FileOutputStream(f1);
                wb1.write(fileOut);   
                fileOut.close();*/                
             	logger.info(path + "/" + weekNo + "_" + storeName + ".txt " + "created successfully...");
        }   
  
       
  public ArrayList<ArrayListRows> sortColoumns(ArrayList<ArrayListRows> rowsList,ArrayList<Integer> tot)   
  {   
      ArrayList<ArrayListRows> rowsArrayList = new ArrayList<ArrayListRows>();   
      for(int i = 0;i<rowsList.size();i++)   
      {   
          ArrayListRows temp = new ArrayListRows();   
          for(int j=0;j<(rowsList.get(i).coloumns.size());j++)   
          {   
              temp.coloumns.add(rowsList.get(i).coloumns.get(tot.get(j)));   
          }   
          rowsArrayList.add(temp);   
      }   
      return rowsArrayList;      
  }   
  public ArrayList<Integer> compareHeadings(ArrayListRows firstRowList1,ArrayListRows firstRowList2)   
  {   
      ArrayList<Integer> tot = new ArrayList<Integer>();   
      if(!(firstRowList1.coloumns.size() == firstRowList2.coloumns.size()))   
      {   
        System.out.println("cannot merge the files");   
        System.exit(0);   
        //return null;   
      }   
      System.out.println(firstRowList1.coloumns);   
      System.out.println(firstRowList2.coloumns);   
     for(int i=0;i<firstRowList1.coloumns.size();i++)   
      {   
         for(int j=0;j<firstRowList2.coloumns.size();j++)   
          {   
                  if((firstRowList1.coloumns.get(i).trim()).equals((firstRowList2.coloumns.get(j).trim())))   
                      {   
                        tot.add(j);   
                       break;   
                      }   
          }   
      }   
     return tot;   
  }   
  public ArrayList<ArrayListRows> addRows(ArrayList<ArrayListRows> rowsArrayList, ArrayList<ArrayListRows> rowsArrayList1 )   
  {   
      ArrayList<ArrayListRows> arrayRows = new ArrayList<ArrayListRows>();   
      Iterator<ArrayListRows> itera = rowsArrayList.iterator();   
      while(itera.hasNext())   
      {   
          arrayRows.add(itera.next());   
      }   
  
     for(int i=0;i<rowsArrayList1.size();i++)   
      {   
          int j;   
         for( j=0;j<rowsArrayList.size();j++)   
         {   
             if(compareColumns(rowsArrayList1.get(i), rowsArrayList.get(j)))   
                     {   
                        break;   
                     }   
                        
         }   
         if(j>=rowsArrayList1.size())   
                arrayRows.add(rowsArrayList1.get(i));   
      }     
      return arrayRows;   
  }   
  public boolean compareColumns(ArrayListRows row1,ArrayListRows row2)   
  {   
      for(int i=0;i<row1.coloumns.size();i++)   
      {   
      if(!row1.coloumns.get(i).equals(row2.coloumns.get(i)))   
          return false;   
      }   
  return true;   
            
  }   
  
  public ArrayList<ArrayListRows> getObjectsFromWorkBook(Workbook wb) throws Exception   
  {   
      ArrayList<ArrayListRows> arrayRows = new ArrayList<ArrayListRows>();   
        Sheet sheet=null;              
       Row row=null;   
        Cell cell=null;   
           
        if(wb!=null){   
            sheet=wb.getSheetAt(0);   
            row=null;   
            cell=null;   
            Iterator<Row> rows=sheet.rowIterator();   
            try {
            	  while(rows.hasNext())   
                  {   
                      ArrayListRows allo = new ArrayListRows();   
                      row=(Row)rows.next();                  
                      Iterator<Cell> cells=row.cellIterator();   
                         
                      while(cells.hasNext())   
                      {   
                      cell=( Cell)cells.next();   
                      if(cell.getCellType()==Cell.CELL_TYPE_STRING){   
                              allo.coloumns.add(cell.getStringCellValue());   
                                 
                      }   
                      else  
                          allo.coloumns.add(""+cell.getNumericCellValue());      
                      }   
                      arrayRows.add(allo);   
                         
                  }
            } catch (Exception e){
            	logger.error("Parse Error " + e);
            }
             
        }   
      return arrayRows;   
  }   
  public  Workbook checkFileFormat(String fileName){   
        Workbook hwb=null;   
        FileInputStream checkFis=null;   
        try{   
             checkFis=new FileInputStream(fileName);   
             //Instantiate the Workbook using HSSFWorkbook   
             hwb=new HSSFWorkbook(checkFis);   
             Sheet sheet=hwb.getSheetAt(0);   
             Iterator<Row> rows=sheet.rowIterator();   
               
             Iterator<Cell> cells=null;   
             Row row=null;   
             Cell cell=null;   
             int check=0;   
             //Read the file as HSSFWorkbook   
             while(rows.hasNext()){   
                check++;   
                row=(HSSFRow)rows.next();   
                cells=row.cellIterator();   
                while(cells.hasNext()){   
                    cell=(HSSFCell)cells.next();   
                }   
                if(check==2)   
                    break;  
                
            }   
            //Return HSSFWorkbook type object if there is no exception in reading the file using HSSFWorkbook   
             
        }catch(ClassCastException ce){  //Instantiate the Workbook using XSSFWorkbook in case of class cast exception   
        	logger.error("Error -- checkFileFormat() in " + fileName, ce);
            }   
        catch(Exception e){ //Instantiate the Workbook using XSSFWorkbook in case of Exception while reading file through HSSFWorkbook   
        	logger.error("Error -- checkFileFormat() in " + fileName, e);
    }   
        return hwb;
    }

  
  public void writeToCellXL(Row row, int colCount, String val, CellStyle style) {
      if (val != null) {
          Cell cell = row.createCell(colCount, HSSFCell.CELL_TYPE_STRING);
          cell.setCellStyle(style);
          cell.setCellValue(val);
      }
  }

private static void logCommand (String[] args)
{
	logger.info("*****************************************");
	StringBuffer sb = new StringBuffer("Command: MovementDataLoad ");
	for ( int ii = 0; ii < args.length; ii++ ) {
		if ( ii > 0 ) sb.append(' ');
		sb.append (args[ii]);
	}
	logger.info(sb.toString());
}

@Override
public void processRecords(List listobj) throws GeneralException {
	// TODO Auto-generated method stub
	
}
}

 