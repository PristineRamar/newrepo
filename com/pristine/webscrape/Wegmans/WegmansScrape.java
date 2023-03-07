
  package com.pristine.webscrape.Wegmans;
 /* 
 * import java.io.FileInputStream; import java.io.FileOutputStream; import
 * java.io.IOException; import java.io.InputStream; import
 * java.net.MalformedURLException; import java.util.ArrayList; import
 * java.util.Iterator; import java.util.List;
 * 
 * import org.apache.log4j.Logger; import org.apache.log4j.PropertyConfigurator;
 * import org.apache.poi.hssf.usermodel.HSSFCell; import
 * org.apache.poi.hssf.usermodel.HSSFRow; import
 * org.apache.poi.hssf.usermodel.HSSFSheet; import
 * org.apache.poi.hssf.usermodel.HSSFWorkbook;
 * 
 * import com.gargoylesoftware.htmlunit.BrowserVersion; import
 * com.gargoylesoftware.htmlunit.WebClient; import
 * com.gargoylesoftware.htmlunit.html.HtmlElement; import
 * com.gargoylesoftware.htmlunit.html.HtmlPage; import
 * com.gargoylesoftware.htmlunit.html.HtmlTable; import
 * com.gargoylesoftware.htmlunit.html.HtmlTableCell; import
 * com.gargoylesoftware.htmlunit.html.HtmlTableRow; import
 * com.pristine.util.PropertyManager; import
 * com.pristine.webscrape.shoprite.ShopriteScrape;
 */ 
  public class WegmansScrape { 
  }
 /*  private static Logger logger =
  * Logger.getLogger("WGMWebScraping"); private WegmansWebScrapeUtil
 * webScrapeUtil = new WegmansWebScrapeUtil(); private static int
 * INPUT_FILE_COL_COUNT = 7; private static int MAX_ITEM_COUNT = 2500;
 * 
 * ArrayList <ScrapeItemDTO> itemList = new ArrayList <ScrapeItemDTO> ();
 *//**
	 * @param args
	 *//*
		 * public static void main(String[] args) { // TODO Auto-generated method stub
		 * PropertyConfigurator.configure("log4j.properties");
		 * PropertyManager.initialize("analysis.properties");
		 * 
		 * if( args.length < 2){
		 * logger.info("Invalid argument: WegmansScrape StoreId FileName");
		 * System.exit(1); } String storeId = args[0]; String fileName = args[1];
		 * logger.info("wegman's Web scraping started ....."); try { WegmansScrape wm =
		 * new WegmansScrape(); wm.readFile(fileName); wm.performScraping(storeId);
		 * wm.writeOutputToExcel("C:/Presto/"+storeId+".xls"); }catch(Exception e){
		 * logger.error("Exception occured",e); }
		 * 
		 * logger.info("Wegman's Web scraping done ....."); }
		 * 
		 * public void performScraping(String strId) throws Exception{ final WebClient
		 * webClient = webScrapeUtil.storeLogin(strId, null, null); //String response =
		 * page.asText(); //logger.info(response);
		 * 
		 * int count = 0; for ( ScrapeItemDTO item :itemList){
		 * webScrapeUtil.scrapeItembyProductCode(webClient, item); count++; if( count ==
		 * MAX_ITEM_COUNT) break; } }
		 * 
		 * 
		 * 
		 * private void readFile(String fileName) throws IOException{ InputStream myxls
		 * = new FileInputStream(fileName); HSSFWorkbook wb = new HSSFWorkbook(myxls);
		 * HSSFSheet sheet = wb.getSheetAt(0); // first sheet
		 * 
		 * if (sheet != null){ int noOfRows = sheet.getPhysicalNumberOfRows(); if(
		 * noOfRows <=0){ logger.info("Spread-sheet is empty"); return; } int
		 * noOfColumns =0; HSSFRow row = sheet.getRow(0); if(row != null) { noOfColumns
		 * = row.getPhysicalNumberOfCells(); } if (noOfColumns < INPUT_FILE_COL_COUNT){
		 * logger.info("Insufficient Header Columns"); return; } //Now read the for (int
		 * i = 1; i < noOfRows; i++){ row = sheet.getRow(i); noOfColumns = 0; if(row !=
		 * null) { noOfColumns = row.getPhysicalNumberOfCells(); } if ( noOfColumns <
		 * INPUT_FILE_COL_COUNT) continue; ScrapeItemDTO wegmansMappingObj = new
		 * ScrapeItemDTO();
		 * 
		 * wegmansMappingObj.dept = row.getCell(0).toString();
		 * wegmansMappingObj.category = row.getCell(1).toString();
		 * wegmansMappingObj.itemName = row.getCell(2).toString();
		 * wegmansMappingObj.friendlyName = row.getCell(3).toString();
		 * wegmansMappingObj.retailerItemCode= row.getCell(4).toString();
		 * wegmansMappingObj.UPC= row.getCell(5).toString();
		 * wegmansMappingObj.wegmansProductCode= row.getCell(6)!= null?
		 * row.getCell(6).toString():null; wegmansMappingObj.regPrice =
		 * row.getCell(9).toString(); wegmansMappingObj.dblRegPrice =
		 * webScrapeUtil.ConvertToDouble(wegmansMappingObj.regPrice); //Don't scrape the
		 * items where Price is "May Not Be Available" if (
		 * wegmansMappingObj.dblRegPrice <= 0) continue; wegmansMappingObj.shoppingURL
		 * =row.getCell(7).toString(); if ( wegmansMappingObj.wegmansProductCode != null
		 * && !wegmansMappingObj.wegmansProductCode.equals("")){
		 * wegmansMappingObj.wegmansProductCode =
		 * wegmansMappingObj.wegmansProductCode.substring(1,
		 * wegmansMappingObj.wegmansProductCode.length()); //if(
		 * wegmansMappingObj.friendlyName.contains("TopCare") ||
		 * wegmansMappingObj.friendlyName.contains("Wegmans"))
		 * itemList.add(wegmansMappingObj); }
		 * 
		 * }
		 * 
		 * logger.info("No Of Mapped Items: " + itemList.size() );
		 * 
		 * }else{ logger.info("Spread-sheet is Invalid");
		 * 
		 * }
		 * 
		 * }
		 * 
		 * private void writeOutputToExcel(String fileName) throws IOException {
		 * 
		 * HSSFWorkbook wb = new HSSFWorkbook(); int rowCount = 0; HSSFSheet sheet =
		 * wb.createSheet(); HSSFRow row = sheet.createRow(rowCount);
		 * createHeaderRow(row); int count = 0; for ( ScrapeItemDTO item :itemList){ if(
		 * item.upcMatch){ rowCount++; row = sheet.createRow(rowCount);
		 * createItemRow(row, item); count++; if( count == MAX_ITEM_COUNT) break; } }
		 * 
		 * FileOutputStream fileOut = new FileOutputStream(fileName); wb.write(fileOut);
		 * fileOut.close(); }
		 * 
		 * private void createHeaderRow(HSSFRow row) { int colCount = 0;
		 * writeToCell(row, colCount, "Dept"); writeToCell(row, ++colCount, "Category");
		 * writeToCell(row, ++colCount, "Item Name"); writeToCell(row, ++colCount,
		 * "Retailer Code"); writeToCell(row, ++colCount, "UPC"); writeToCell(row,
		 * ++colCount, "Weg Item Name"); writeToCell(row, ++colCount, "Weg Size");
		 * writeToCell(row, ++colCount, "Reg Rtl"); writeToCell(row, ++colCount,
		 * "Promo Rtl"); }
		 * 
		 * private void createItemRow(HSSFRow row, ScrapeItemDTO item){ int colCount =
		 * 0; writeToCell(row, colCount, item.dept); writeToCell(row, ++colCount,
		 * item.category); writeToCell(row, ++colCount, item.itemName); writeToCell(row,
		 * ++colCount, item.retailerItemCode); writeToCell(row, ++colCount, item.UPC);
		 * writeToCell(row, ++colCount, item.wegmansItemName); writeToCell(row,
		 * ++colCount, item.itemUOMSize); writeToCell(row, ++colCount, item.regPrice);
		 * writeToCell(row, ++colCount, item.salePrice); }
		 * 
		 * private void writeToCell( HSSFRow row, int colCount, String val) {
		 * webScrapeUtil.writeToCell( row, colCount, val); } }
		 */