
  package com.pristine.webscrape.Wegmans;
 /* 
 * import java.io.FileInputStream; import java.io.FileOutputStream; import
 * java.io.IOException; import java.io.InputStream; import
 * java.net.MalformedURLException; import java.util.ArrayList; import
 * java.util.Collection; import java.util.HashMap; import java.util.Iterator;
 * import java.util.List;
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
 * com.pristine.exception.GeneralException; import com.pristine.util.DateUtil;
 * 
 * import com.pristine.util.PropertyManager; import java.io.File; import
 * java.net.SocketException; import java.sql.Timestamp; import java.util.Date;
 * 
 * 
 */ 
  public class WegmansScrapeV2 { }
 /* private static Logger logger =
 * Logger.getLogger("WGMWebScraping"); private WegmansWebScrapeUtil
 * webScrapeUtil = new WegmansWebScrapeUtil(); private static int
 * INPUT_FILE_COL_COUNT = 7; private static int MAX_ITEM_COUNT = 1000; private
 * int DELAY_PERIOD = 10000; private String compStrId; private String
 * currentDate;
 * 
 * FileOutputStream fileOut; //ArrayList <ScrapeItemDTO> itemList = new
 * ArrayList <ScrapeItemDTO> (); private HashMap<String,
 * ArrayList<ScrapeItemDTO>> itemScrapeMap = new HashMap<String,
 * ArrayList<ScrapeItemDTO>>();
 *//**
	 * @param args
	 *//*
		 * public static void main(String[] args) { // TODO Auto-generated method stub
		 * PropertyConfigurator.configure("log4j.properties");
		 * PropertyManager.initialize("analysis.properties");
		 * 
		 * if( args.length < 2){
		 * LogProcessInfo("Invalid argument: WegmansScrape StoreId FileName");
		 * System.exit(1); } String storeId = args[0]; String fileName = args[1];
		 * LogProcessInfo("wegman's Web scraping started ....."); WegmansScrapeV2 wm =
		 * new WegmansScrapeV2(); try {
		 * 
		 * wm.readFile(new FileInputStream(fileName)); wm.performScraping(storeId);
		 * 
		 * }catch(Exception e){ logger.error("Exception occured",e); } try{
		 * wm.writeOutputToCSV(new File("C:/Presto/"+storeId+".csv"));
		 * }catch(GeneralException e){ logger.error("Exception occured",e);
		 * }catch(IOException ioe){ logger.error("IO Exception occured",ioe); }
		 * 
		 * 
		 * LogProcessInfo("Wegman's Web scraping done ....."); }
		 * 
		 * public void performScraping(String strId) throws Exception{ final WebClient
		 * webClient = webScrapeUtil.storeLogin(strId, null, null); //String response =
		 * page.asText(); // LogProcessInfo(response); compStrId = strId; currentDate =
		 * DateUtil.getDateFromCurrentDate(0); Iterator<String> keyItr =
		 * itemScrapeMap.keySet().iterator(); ArrayList<ScrapeItemDTO>
		 * wegmanItemListArray = new ArrayList<ScrapeItemDTO> (); int hitCounter = 0;
		 * int count = 0; int noOfPrices = 0; String shoppingURL =""; boolean
		 * retry=false; while ( keyItr.hasNext()){
		 * 
		 * try { if(!retry) shoppingURL=keyItr.next(); else retry=false;
		 * ArrayList<ScrapeItemDTO> priceCheckItemList = itemScrapeMap.get(shoppingURL);
		 * 
		 * if( priceCheckItemList.size() >2){ //String response = page.asText();
		 * HtmlPage page = webClient.getPage(shoppingURL);
		 * //webScrapeUtil.printPagition(page); hitCounter++; List<HtmlTableRow>
		 * tblRowList = webScrapeUtil.getItemListTable(page);
		 * 
		 * webScrapeUtil.setReferenceData(null, shoppingURL,false);
		 * wegmanItemListArray.clear();
		 * webScrapeUtil.processItemTableList(webClient,tblRowList,
		 * wegmanItemListArray); noOfPrices += copyPrices(wegmanItemListArray,
		 * priceCheckItemList); //introduce delay
		 * LogProcessInfo("Under If Cond Timestamp- " + DateUtil.getCurrentDateTime()+
		 * "- NoofPrices--"+noOfPrices); Thread.sleep(DELAY_PERIOD); }
		 * 
		 * for ( ScrapeItemDTO item :priceCheckItemList){ //Convert the prices to double
		 * item.dblRegPrice = webScrapeUtil.ConvertToDouble(item.regPrice);
		 * 
		 * if( item.dblRegPrice < 0.01d){
		 * webScrapeUtil.scrapeItembyProductCode(webClient, item); hitCounter++;
		 * noOfPrices++; //introduce delay LogProcessInfo("Under For Loop Timestamp- " +
		 * DateUtil.getCurrentDateTime()+ "- NoofPrices--"+noOfPrices);
		 * Thread.sleep(DELAY_PERIOD/3); }else{ //Update Hit counter double dblSalePrice
		 * = webScrapeUtil.ConvertToDouble(item.salePrice); if ( dblSalePrice > 0)
		 * hitCounter++;
		 * 
		 * 
		 * }
		 * 
		 * } count++; if( count >= MAX_ITEM_COUNT) break; } catch(SocketException ex) {
		 * LogProcessInfo("Socket Exception --"+ex.getMessage()
		 * +"-\n Please wait for 3 Minutes to reattempt"); Thread.sleep(1000*60*3);
		 * retry=true; }
		 * 
		 * }
		 * 
		 * LogProcessInfo("No of Hits = " + hitCounter);
		 * LogProcessInfo("No of Prices = " + noOfPrices);
		 * 
		 * 
		 * }
		 * 
		 * private int copyPrices(ArrayList<ScrapeItemDTO> wegmanItemListArray,
		 * ArrayList<ScrapeItemDTO> priceCheckItemList){
		 * 
		 * int noOfPrices = 0; for ( ScrapeItemDTO wegitem :wegmanItemListArray){
		 * wegitem.dblRegPrice = webScrapeUtil.ConvertToDouble(wegitem.regPrice); if (
		 * wegitem.dblRegPrice > 0){ for ( ScrapeItemDTO item :priceCheckItemList){ if(
		 * wegitem.UPC.contains(item.UPC) ||
		 * wegitem.wegmansProductCode.equals(item.wegmansProductCode)){ item.upcMatch =
		 * true; item.itemUOMSize = wegitem.itemUOMDesc; item.regPrice =
		 * wegitem.regPrice; item.salePrice = wegitem.salePrice; item.wegmansItemName =
		 * wegitem.itemName;
		 * 
		 * noOfPrices++; }
		 * 
		 * } } } return noOfPrices; }
		 * 
		 * 
		 * public void readFile(InputStream myxls) throws IOException{ HSSFWorkbook wb =
		 * new HSSFWorkbook(myxls); HSSFSheet sheet = wb.getSheetAt(0); // first sheet
		 * 
		 * if (sheet != null){ int noOfRows = sheet.getPhysicalNumberOfRows(); if(
		 * noOfRows <=0){ LogProcessInfo("Spread-sheet is empty"); return; } int
		 * noOfColumns =0; HSSFRow row = sheet.getRow(0); if(row != null) { noOfColumns
		 * = row.getPhysicalNumberOfCells(); } if (noOfColumns < INPUT_FILE_COL_COUNT){
		 * LogProcessInfo("Insufficient Header Columns"); return; } //Now read the for
		 * (int i = 1; i < noOfRows; i++){ row = sheet.getRow(i); noOfColumns = 0;
		 * if(row != null) { noOfColumns = row.getPhysicalNumberOfCells(); } if (
		 * noOfColumns < INPUT_FILE_COL_COUNT) continue; ScrapeItemDTO wegmansMappingObj
		 * = new ScrapeItemDTO();
		 * 
		 * wegmansMappingObj.dept = row.getCell(0).toString();
		 * wegmansMappingObj.category = row.getCell(1).toString();
		 * wegmansMappingObj.itemName = row.getCell(2).toString();
		 * wegmansMappingObj.friendlyName = row.getCell(3).toString();
		 * wegmansMappingObj.retailerItemCode= row.getCell(4).toString();
		 * wegmansMappingObj.UPC= row.getCell(5).toString(); wegmansMappingObj.UPC=
		 * wegmansMappingObj.UPC.replaceAll("'", "");
		 * wegmansMappingObj.wegmansProductCode= row.getCell(6)!= null?
		 * row.getCell(6).toString():null; //wegmansMappingObj.regPrice =
		 * row.getCell(9).toString(); //wegmansMappingObj.dblRegPrice =
		 * webScrapeUtil.ConvertToDouble(wegmansMappingObj.regPrice); //Don't scrape the
		 * items where Price is "May Not Be Available" //if (
		 * wegmansMappingObj.dblRegPrice <= 0) // continue;
		 * wegmansMappingObj.shoppingURL =row.getCell(7).toString(); if (
		 * wegmansMappingObj.wegmansProductCode != null &&
		 * !wegmansMappingObj.wegmansProductCode.equals("")){
		 * wegmansMappingObj.wegmansProductCode =
		 * wegmansMappingObj.wegmansProductCode.substring(1,
		 * wegmansMappingObj.wegmansProductCode.length()); //If is to filter Private
		 * label items only. //if( wegmansMappingObj.friendlyName.contains("TopCare") ||
		 * wegmansMappingObj.friendlyName.contains("Wegmans")) ArrayList <ScrapeItemDTO>
		 * itemList; if( itemScrapeMap.containsKey(wegmansMappingObj.shoppingURL)){
		 * itemList = itemScrapeMap.get(wegmansMappingObj.shoppingURL);
		 * itemList.add(wegmansMappingObj); }else{ itemList = new ArrayList
		 * <ScrapeItemDTO>(); itemList.add(wegmansMappingObj);
		 * itemScrapeMap.put(wegmansMappingObj.shoppingURL, itemList);
		 * 
		 * } }
		 * 
		 * }
		 * 
		 * LogProcessInfo("No Of Hits Mapped (Best Case): " + itemScrapeMap.size() );
		 * 
		 * }else{ LogProcessInfo("Spread-sheet is Invalid");
		 * 
		 * }
		 * 
		 * } private static void LogProcessInfo(String processinfo) {
		 * 
		 * logger.info(processinfo); //
		 * ProgressStatusThread.UpdateProgress(processinfo);
		 * 
		 * logger.info(processinfo); //ProgressStatusThread.UpdateProgress(processinfo);
		 * 
		 * } public void writeOutputToCSV(File fileName) throws IOException,
		 * GeneralException {
		 * 
		 * 
		 * fileOut = new FileOutputStream(fileName);
		 * 
		 * HSSFWorkbook wb = new HSSFWorkbook(); int rowCount = 0; HSSFSheet sheet =
		 * wb.createSheet(); HSSFRow row = sheet.createRow(rowCount);
		 * createHeaderRow(row); int count = 0;
		 * 
		 * Collection<ArrayList<ScrapeItemDTO>> itemCollection = itemScrapeMap.values();
		 * Iterator<ArrayList<ScrapeItemDTO>> itrCollection = itemCollection.iterator();
		 * 
		 * 
		 * while( itrCollection.hasNext()){ ArrayList<ScrapeItemDTO> itemList =
		 * itrCollection.next(); for ( ScrapeItemDTO item :itemList){ if(
		 * item.upcMatch){ rowCount++; row = sheet.createRow(rowCount);
		 * createItemRow(row, item); count++; } } }
		 * 
		 * //FileOutputStream fileOut = new FileOutputStream(fileName);
		 * //wb.write(fileOut); fileOut.close(); }
		 * 
		 * private void createHeaderRow(HSSFRow row) throws GeneralException { int
		 * colCount = 0;
		 * 
		 * writeToCell(row, colCount, "Store Num"); writeToCell(row, ++colCount,
		 * "Check Date"); writeToCell(row, ++colCount, "Item Code"); writeToCell(row,
		 * ++colCount, "Item Desc"); writeToCell(row, ++colCount, "Reg Quantity");
		 * writeToCell(row, ++colCount, "Reg Price"); writeToCell(row, ++colCount,
		 * "Size"); writeToCell(row, ++colCount, "UPC"); writeToCell(row, ++colCount,
		 * "Outside Indicator"); writeToCell(row, ++colCount, "Additional Info");
		 * writeToCell(row, ++colCount, "Sale Quantity"); writeToCell(row, ++colCount,
		 * "Sale Price"); writeToCell(row, ++colCount, "Sale Date"); writeToCell(row,
		 * ++colCount, "Wegmans Item Name"); writeRowToFile();
		 * 
		 * }
		 * 
		 * private void createItemRow(HSSFRow row, ScrapeItemDTO item) throws
		 * GeneralException { int colCount = 0; writeToCell(row, colCount, compStrId);
		 * writeToCell(row, ++colCount, currentDate); writeToCell(row, ++colCount,
		 * "NA"); writeToCell(row, ++colCount, item.itemName.replaceAll(",", " "));
		 * 
		 * String additionalInfo = ""; //RegQty and Price writeToCell(row, ++colCount,
		 * Integer.toString(webScrapeUtil.getQuantity(item.regPrice))); writeToCell(row,
		 * ++colCount, Double.toString(webScrapeUtil.getPrice(item.regPrice)));
		 * 
		 * writeToCell(row, ++colCount, item.itemUOMSize); writeToCell(row, ++colCount,
		 * item.UPC); writeToCell(row, ++colCount, "N"); writeToCell(row, ++colCount,
		 * additionalInfo);
		 * 
		 * writeToCell(row, ++colCount,
		 * Integer.toString(webScrapeUtil.getQuantity(item.salePrice)));
		 * writeToCell(row, ++colCount,
		 * Double.toString(webScrapeUtil.getPrice(item.salePrice)));
		 * 
		 * 
		 * //SaleQty and Price writeToCell(row, ++colCount, ""); writeToCell(row,
		 * ++colCount, item.wegmansItemName.replaceAll(",", " ")); writeRowToFile(); }
		 * 
		 * StringBuffer sb = new StringBuffer (); private void writeToCell( HSSFRow row,
		 * int colCount, String val) { //webScrapeUtil.writeToCell( row, colCount, val);
		 * if( colCount == 0) sb = new StringBuffer (); else sb.append(",");
		 * sb.append(val);
		 * 
		 * }
		 * 
		 * private void writeRowToFile() throws GeneralException{ try{
		 * fileOut.write(sb.toString().getBytes()); fileOut.write('\n'); }catch
		 * (IOException ioe){ throw new GeneralException ("File Operation Exception",
		 * ioe); } } }
		 */