
  package com.pristine.webscrape.Wegmans;
 /* 
 * import java.io.FileInputStream; import java.io.FileOutputStream; import
 * java.io.IOException; import java.io.InputStream; import java.util.ArrayList;
 * import java.util.HashMap; import java.util.Map;
 * 
 * import org.apache.log4j.Logger; import org.apache.poi.hssf.usermodel.HSSFRow;
 * import org.apache.poi.hssf.usermodel.HSSFSheet; import
 * org.apache.poi.hssf.usermodel.HSSFWorkbook;
 */
  public class WegmansProductCodeMap { }
 /* private static Logger logger =
 * Logger.getLogger("WegmansProductCodeMap"); private WegmansWebScrapeUtil
 * scrapeUtil = new WegmansWebScrapeUtil(); private static boolean filterOutput
 * = true;
 *//**
	 * @param args
	 *//*
		 * public static void main(String[] args) { // TODO Auto-generated method stub
		 * 
		 * if( args.length < 3){ logger.
		 * info("Invalid argument: WegmansProductCodeMap ss_department ss_file wegmans_file true/false"
		 * ); System.exit(1); } try{
		 * logger.info("Wegmans Produce Code Mapping started ...");
		 * WegmansProductCodeMap productCodeMap = new WegmansProductCodeMap(); if(
		 * args.length > 3){ filterOutput = Boolean.valueOf(args[3]).booleanValue(); }
		 * 
		 * productCodeMap.doMapping(args[0], args[1], args[2]);
		 * logger.info("Wegmans Produce Code Mapping Done ..."); }catch (Exception e){
		 * logger.error("Exception in Product scraping", e); }
		 * 
		 * }
		 * 
		 * private void doMapping(String deptName, String ssDataFile, String
		 * wegDataFileName) throws Exception{ ArrayList <ScrapeItemDTO> ssItemList =
		 * readSSDataFile(deptName, ssDataFile); if( ssItemList == null ||
		 * ssItemList.size() == 0){
		 * logger.info("SS Input File is invalid or empty!!! cannot proceed...");
		 * return; }
		 * 
		 * ArrayList <ScrapeItemDTO> wegItemList = readWegDataFile(wegDataFileName); if(
		 * wegItemList == null || wegItemList.size() == 0){
		 * logger.info("Wegman Input File is invalid or empty!!! cannot proceed...");
		 * return; }
		 * 
		 * HashMap<String, ArrayList<ScrapeItemDTO>> itemMap = doItemMapping(ssItemList,
		 * wegItemList); if( itemMap == null || itemMap.size() == 0){
		 * logger.info("No mapping present!!! cannot proceed..."); return; } String
		 * fileName = "C:/Presto/GenMap/"+deptName;
		 * 
		 * if(filterOutput) fileName = fileName + "_Filtered_Map.xls"; else fileName=
		 * fileName+"_Full_Map.xls";
		 * 
		 * PrintItemMap(fileName, itemMap);
		 * 
		 * }
		 * 
		 * private ArrayList <ScrapeItemDTO> readSSDataFile(String deptName, String
		 * ssDataFile) throws IOException { ArrayList <ScrapeItemDTO> itemList = new
		 * ArrayList <ScrapeItemDTO> ();
		 * 
		 * InputStream myxls = new FileInputStream(ssDataFile); HSSFWorkbook wb = new
		 * HSSFWorkbook(myxls); HSSFSheet sheet = wb.getSheetAt(0); // first sheet int
		 * INPUT_FILE_COL_COUNT = 9;
		 * 
		 * if (sheet != null){ int noOfRows = sheet.getPhysicalNumberOfRows(); if(
		 * noOfRows <=0){ logger.info("Spread-sheet is empty"); return null; } int
		 * noOfColumns =0; HSSFRow row = sheet.getRow(0); if(row != null) { noOfColumns
		 * = row.getPhysicalNumberOfCells(); } if (noOfColumns < INPUT_FILE_COL_COUNT){
		 * logger.info("Insufficient Header Columns"); return null; } //Now read the for
		 * (int i = 1; i < noOfRows; i++){ row = sheet.getRow(i); noOfColumns = 0;
		 * if(row != null) { noOfColumns = row.getPhysicalNumberOfCells(); } if (
		 * noOfColumns < INPUT_FILE_COL_COUNT) continue; ScrapeItemDTO ssItemInfo = new
		 * ScrapeItemDTO();
		 * 
		 * ssItemInfo.dept = row.getCell(1).toString(); ssItemInfo.UPC=
		 * row.getCell(2).toString(); ssItemInfo.UPC =
		 * scrapeUtil.standardizeUPC(ssItemInfo.UPC); ssItemInfo.itemName =
		 * row.getCell(3).toString(); ssItemInfo.itemUOMSize= row.getCell(4).toString();
		 * ssItemInfo.minPrice= scrapeUtil.ConvertToDouble(row.getCell(5).toString());
		 * ssItemInfo.maxPrice= scrapeUtil.ConvertToDouble(row.getCell(6).toString());
		 * ssItemInfo.modePrice= scrapeUtil.ConvertToDouble(row.getCell(7).toString());
		 * ssItemInfo.wegmansPriceFlg= row.getCell(8).toString();
		 * 
		 * if( ssItemInfo.dept.equalsIgnoreCase(deptName)){ itemList.add(ssItemInfo);
		 * //split Item words and UOM scrapeUtil.splitUOM(ssItemInfo.itemUOMSize,
		 * ssItemInfo); ssItemInfo.dblSize =
		 * scrapeUtil.ConvertToDouble(ssItemInfo.itemSize);
		 * //logger.info(ssItemInfo.itemName+ "-" + ssItemInfo.itemSize + "-" +
		 * ssItemInfo.itemUOMDesc); ssItemInfo.itemWords =
		 * ssItemInfo.itemName.split(" ");
		 * scrapeUtil.cleanupWords(ssItemInfo.itemWords);
		 * 
		 * } }
		 * 
		 * logger.info("No Of SS Items in " + deptName+ " = " + itemList.size() );
		 * 
		 * }else{ logger.info("Spread-sheet is Invalid");
		 * 
		 * } return itemList; }
		 * 
		 * private ArrayList <ScrapeItemDTO> readWegDataFile(String wegDataFileName)
		 * throws IOException { ArrayList <ScrapeItemDTO> itemList = new ArrayList
		 * <ScrapeItemDTO> ();
		 * 
		 * InputStream myxls = new FileInputStream(wegDataFileName); HSSFWorkbook wb =
		 * new HSSFWorkbook(myxls); HSSFSheet sheet = wb.getSheetAt(0); // first sheet
		 * int INPUT_FILE_COL_COUNT = 8;
		 * 
		 * if (sheet != null){ int noOfRows = sheet.getPhysicalNumberOfRows(); if(
		 * noOfRows <=0){ logger.info("Spread-sheet is empty"); return null; } int
		 * noOfColumns =0; HSSFRow row = sheet.getRow(0); if(row != null) { noOfColumns
		 * = row.getPhysicalNumberOfCells(); } if (noOfColumns < INPUT_FILE_COL_COUNT){
		 * logger.info("Insufficient Header Columns"); return null; } //Now read the for
		 * (int i = 1; i < noOfRows; i++){ row = sheet.getRow(i); noOfColumns = 0;
		 * if(row != null) { noOfColumns = row.getPhysicalNumberOfCells(); } if (
		 * noOfColumns < INPUT_FILE_COL_COUNT) continue; ScrapeItemDTO wegmanItemInfo =
		 * new ScrapeItemDTO();
		 * 
		 * wegmanItemInfo.dept = row.getCell(0).toString(); wegmanItemInfo.itemName =
		 * row.getCell(1).toString(); wegmanItemInfo.itemSize=
		 * row.getCell(2).toString(); wegmanItemInfo.itemUOMDesc =
		 * row.getCell(3).toString(); wegmanItemInfo.regPrice =
		 * row.getCell(4).toString(); if( row.getCell(5)!= null)
		 * wegmanItemInfo.salePrice = row.getCell(5).toString();
		 * wegmanItemInfo.wegmansProductCode= row.getCell(6).toString();
		 * wegmanItemInfo.UPC= row.getCell(7).toString(); wegmanItemInfo.shoppingURL=
		 * row.getCell(8).toString();
		 * 
		 * wegmanItemInfo.itemWords = wegmanItemInfo.itemName.split(" ");
		 * scrapeUtil.cleanupWords(wegmanItemInfo.itemWords);
		 * wegmanItemInfo.dblRegPrice=
		 * scrapeUtil.ConvertToDouble(wegmanItemInfo.regPrice); //convert size to
		 * numeric wegmanItemInfo.dblSize =
		 * scrapeUtil.ConvertToDouble(wegmanItemInfo.itemSize); //normalize UOM
		 * wegmanItemInfo.itemUOMDesc =
		 * scrapeUtil.normalizeUOM(wegmanItemInfo.itemUOMDesc);
		 * itemList.add(wegmanItemInfo);
		 * //scrapeUtil.splitUOM(wegmanItemInfo.itemUOMSize, wegmanItemInfo);
		 * 
		 * }
		 * 
		 * logger.info("No Of Wegmans Items = " + itemList.size() );
		 * 
		 * }else{ logger.info("Spread-sheet is Invalid");
		 * 
		 * } return itemList;
		 * 
		 * }
		 * 
		 * //do matching //size matches, price in range, uom matches //foreach ss_item,
		 * rank = 1 for price match, for each word match add 1 //rank 2 or more
		 * 
		 * //First item in the map would be S&S item. Other items are //If the first
		 * word is Care-one replace it Wegmans or TopCare are for Private label items.
		 * 
		 * private HashMap<String, ArrayList<ScrapeItemDTO>> doItemMapping(
		 * ArrayList<ScrapeItemDTO> ssItemList, ArrayList<ScrapeItemDTO>
		 * wegManItemList){ HashMap<String, ArrayList<ScrapeItemDTO>> itemMap = new
		 * HashMap <String, ArrayList<ScrapeItemDTO>> (); int MINIMUM_SCORE = 2; for (
		 * ScrapeItemDTO ssItem :ssItemList){ ArrayList<ScrapeItemDTO> mapItemList = new
		 * ArrayList<ScrapeItemDTO> (); mapItemList.add(ssItem);
		 * 
		 * for ( ScrapeItemDTO wegItem :wegManItemList){
		 * 
		 * 
		 * int score = performItemMatch( ssItem, wegItem); if( (score >= MINIMUM_SCORE
		 * && wegItem.sizeMatch) || (score >= MINIMUM_SCORE && wegItem.priceMatch)){
		 * 
		 * //logger.info( wegItem.itemName + " " + wegItem.itemSize+ " Score = "
		 * +score); ScrapeItemDTO cloneWegItem = wegItem.clone(); cloneWegItem.score =
		 * score; cloneWegItem.pctScore = score* 100/ssItem.itemWords.length;;
		 * mapItemList.add(cloneWegItem);
		 * 
		 * //logger.info(ssItem.itemName + " - " + ssItem.itemSize + ssItem.itemUOMDesc
		 * + " - "+ ssItem.maxPrice+ // "->" + wegItem.itemName + " - " +
		 * wegItem.itemSize + wegItem.itemUOMDesc + " - "+ wegItem.regPrice); } } if (
		 * mapItemList.size()>1) itemMap.put(ssItem.UPC, mapItemList); }
		 * 
		 * logger.info( "No of Possible Items Mapped =" + itemMap.size());
		 * 
		 * return itemMap; }
		 * 
		 * private int performItemMatch( ScrapeItemDTO ssItem, ScrapeItemDTO wegItem){
		 * int score = 0; double minPrice = ssItem.minPrice *.95; double maxPrice =
		 * ssItem.maxPrice * 1.05;
		 * 
		 * //For Debugging purpose
		 * 
		 * if (ssItem.itemName.equals("CARE-ONE JEWEL TAMPON SCENT REGULAR") &&
		 * wegItem.itemName.equals("TopCare Tampons, Regular Absorbency, Scented"))
		 * score = 0;
		 * 
		 * 
		 * if( (ssItem.dblSize == wegItem.dblSize) &&
		 * ssItem.itemUOMDesc.equalsIgnoreCase(wegItem.itemUOMDesc)) wegItem.sizeMatch =
		 * true; else { //wegItem.sizeMatch = false; wegItem.sizeMatch =
		 * checkUOMMatch(ssItem.itemUOMDesc, ssItem.dblSize, wegItem.itemUOMDesc,
		 * wegItem.dblSize); }
		 * 
		 * 
		 * 
		 * 
		 * if ( wegItem.dblRegPrice >= minPrice && wegItem.dblRegPrice <= maxPrice ){
		 * 
		 * if( wegItem.dblRegPrice == ssItem.minPrice || wegItem.dblRegPrice ==
		 * ssItem.maxPrice || wegItem.dblRegPrice == ssItem.modePrice ){
		 * wegItem.priceMatch = true; } else{ //wegItem.priceMatch = false;
		 * wegItem.priceMatch = true; }
		 * 
		 * if( ssItem.itemWords[0].equalsIgnoreCase("CAREONE") ||
		 * ssItem.itemWords[0].equalsIgnoreCase("CARE-ONE")){ if( doesWordMatch(
		 * "Wegmans", wegItem.itemWords)) score++; else if ( doesWordMatch( "TopCare",
		 * wegItem.itemWords)) score++; } else if(
		 * ssItem.itemWords[0].equalsIgnoreCase("GIANT")){ if( doesWordMatch( "Wegmans",
		 * wegItem.itemWords)) score++; else if ( doesWordMatch( "TopCare",
		 * wegItem.itemWords)) score++; } else if(
		 * ssItem.itemWords[0].equalsIgnoreCase("SMARTLIVING") ||
		 * ssItem.itemWords[0].equalsIgnoreCase("SMART-LIVING")){ if( doesWordMatch(
		 * "Wegmans", wegItem.itemWords)) score++; else if ( doesWordMatch( "TopCare",
		 * wegItem.itemWords)) score++; } else if(
		 * ssItem.itemWords[0].equalsIgnoreCase(wegItem.itemWords[0])) score++;
		 * 
		 * if( score > 0) wegItem.brandMatch = true; else wegItem.brandMatch = false;
		 * 
		 * for( int i = 1; i < ssItem.itemWords.length; i++){ if( doesWordMatch(
		 * ssItem.itemWords[i], wegItem.itemWords)) score++; }
		 * 
		 * } return score; }
		 * 
		 * private boolean doesWordMatch( String actualWord, String[] wordArray){
		 * boolean wordMatch = false; String [] reservedWords= {"&"};
		 * 
		 * boolean reservedWordMatch = false; for( int i = 0; i < reservedWords.length;
		 * i++ ){ if( actualWord.equalsIgnoreCase(reservedWords[i])){ reservedWordMatch
		 * = true; break; } }
		 * 
		 * if( !reservedWordMatch){ boolean actualWordPlural = isPlural(actualWord);
		 * 
		 * 
		 * for( int i = 0; i < wordArray.length; i++ ){ if(
		 * actualWord.equalsIgnoreCase(wordArray[i])){ wordMatch = true; break; }
		 * boolean matchWordPlural = isPlural(wordArray[i]); if( actualWordPlural &&
		 * !matchWordPlural){ String matchWord = wordArray[i] + "S"; if(
		 * actualWord.equalsIgnoreCase(matchWord)){ wordMatch = true; break; } }else if
		 * ( !actualWordPlural && matchWordPlural){ String plActualWord = actualWord +
		 * "S"; if( plActualWord.equalsIgnoreCase(wordArray[i])){ wordMatch = true;
		 * break; } } }
		 * 
		 * } return wordMatch; }
		 * 
		 * private boolean isPlural(String word){ boolean plural = false; int length =
		 * word.length();
		 * 
		 * if( (length > 0) && ((word.charAt(length - 1)=='S') || (word.charAt(length -
		 * 1)=='s'))) plural = true; return plural; } private boolean checkUOMMatch(
		 * String item1UOMDesc, double dblItem1Size, String item2UOMDesc, double
		 * dblItem2Size ){ boolean sizeMatch = false;
		 * 
		 * boolean isItem1OZLB = false; boolean isItem2OZLB = false;
		 * 
		 * if( item1UOMDesc.equalsIgnoreCase("LB")||item1UOMDesc.equalsIgnoreCase("OZ"))
		 * isItem1OZLB = true; if(
		 * item2UOMDesc.equalsIgnoreCase("LB")||item2UOMDesc.equalsIgnoreCase("OZ"))
		 * isItem2OZLB = true;
		 * 
		 * if( isItem1OZLB && isItem2OZLB){ if( item1UOMDesc.equalsIgnoreCase("LB"))
		 * dblItem1Size = dblItem1Size * 16;
		 * 
		 * if( item2UOMDesc.equalsIgnoreCase("LB")) dblItem2Size = dblItem2Size * 16;
		 * 
		 * if( dblItem1Size == dblItem2Size) sizeMatch = true; }
		 * 
		 * 
		 * //Fluid ounces to GAl conversion boolean isItem1FOZGAL = false; boolean
		 * isItem2FOZGAL = false;
		 * 
		 * if(
		 * item1UOMDesc.equalsIgnoreCase("FOZ")||item1UOMDesc.equalsIgnoreCase("GAL"))
		 * isItem1FOZGAL = true; if(
		 * item2UOMDesc.equalsIgnoreCase("FOZ")||item2UOMDesc.equalsIgnoreCase("GAL"))
		 * isItem2FOZGAL = true;
		 * 
		 * if( isItem1FOZGAL && isItem2FOZGAL){ if(
		 * item1UOMDesc.equalsIgnoreCase("GAL")) dblItem1Size = dblItem1Size * 128;
		 * 
		 * if( item2UOMDesc.equalsIgnoreCase("GAL")) dblItem2Size = dblItem2Size * 128;
		 * 
		 * if( dblItem1Size == dblItem2Size) sizeMatch = true; }
		 * 
		 * 
		 * 
		 * double MLITER_TO_FOZ = 0.0338140227;
		 * 
		 * //Fluid ounces to GAl conversion boolean isItem1FOZLT = false; boolean
		 * isItem2FOZLT = false;
		 * 
		 * if( item1UOMDesc.equalsIgnoreCase("FOZ")||item1UOMDesc.equalsIgnoreCase("LT")
		 * ||item1UOMDesc.equalsIgnoreCase("MLT")) isItem1FOZLT = true; if(
		 * item2UOMDesc.equalsIgnoreCase("FOZ")||item2UOMDesc.equalsIgnoreCase("LT")
		 * ||item2UOMDesc.equalsIgnoreCase("MLT")) isItem2FOZLT = true;
		 * 
		 * if( isItem1FOZLT && isItem2FOZLT){ if( item1UOMDesc.equalsIgnoreCase("LT"))
		 * dblItem1Size = dblItem1Size * 1000;
		 * 
		 * if( item2UOMDesc.equalsIgnoreCase("LT")) dblItem2Size = dblItem2Size * 1000;
		 * 
		 * 
		 * if( item1UOMDesc.equalsIgnoreCase("LT") ||
		 * item1UOMDesc.equalsIgnoreCase("MLT")) dblItem1Size = dblItem1Size *
		 * MLITER_TO_FOZ;
		 * 
		 * if( item2UOMDesc.equalsIgnoreCase("LT") ||
		 * item2UOMDesc.equalsIgnoreCase("MLT")) dblItem2Size = dblItem2Size *
		 * MLITER_TO_FOZ;
		 * 
		 * if( Math.abs(dblItem1Size - dblItem2Size) < 0.1) sizeMatch = true; }
		 * 
		 * 
		 * return sizeMatch; }
		 * 
		 * private void PrintItemMap(String fileName, HashMap<String,
		 * ArrayList<ScrapeItemDTO>> itemMap) throws IOException { HSSFWorkbook wb = new
		 * HSSFWorkbook(); int rowCount = 0; HSSFSheet sheet = wb.createSheet(); HSSFRow
		 * row = sheet.createRow(rowCount); createHeaderRow(row); int count = 0;
		 * 
		 * 
		 * for(Map.Entry<String, ArrayList<ScrapeItemDTO>> entry : itemMap.entrySet()){
		 * 
		 * ArrayList<ScrapeItemDTO> mappedItemsList = entry.getValue(); //note: 0th
		 * index is SS item and other items are wegman items ScrapeItemDTO ssItem =
		 * mappedItemsList.get(0); mappedItemsList.remove(0);
		 * 
		 * //TO_DO if the flag is set, then filter the list to have the best result.
		 * if(filterOutput) mappedItemsList = filterBestMatch(ssItem,mappedItemsList);
		 * rowCount++; int mapCount = mappedItemsList.size(); for( int i = 0; i <
		 * mapCount; i++){ rowCount++; row = sheet.createRow(rowCount);
		 * createItemRow(row, ssItem, mappedItemsList.get(i), mapCount); }
		 * 
		 * }
		 * 
		 * for ( ScrapeItemDTO item :wegmanItemList){ rowCount++; row =
		 * sheet.createRow(rowCount); createItemRow(row, item); count++; //if( count ==
		 * MAX_ITEM_COUNT) break; }
		 * 
		 * 
		 * FileOutputStream fileOut = new FileOutputStream(fileName); wb.write(fileOut);
		 * fileOut.close(); }
		 * 
		 * private void createHeaderRow(HSSFRow row) { int colCount = 0;
		 * writeToCell(row, colCount, "Dept"); writeToCell(row, ++colCount, "Category");
		 * writeToCell(row, ++colCount, "SS Item Name"); writeToCell(row, ++colCount,
		 * "Wegman Item Name"); writeToCell(row, ++colCount, "Retailer Code");
		 * writeToCell(row, ++colCount, "UPC"); writeToCell(row, ++colCount,
		 * "Product Code"); writeToCell(row, ++colCount, "URL"); writeToCell(row,
		 * ++colCount, "SS Price"); writeToCell(row, ++colCount, "Wegman Price");
		 * writeToCell(row, ++colCount, "Promo Price"); writeToCell(row, ++colCount,
		 * "Score"); writeToCell(row, ++colCount, "PCT Score"); writeToCell(row,
		 * ++colCount, "Price Match"); writeToCell(row, ++colCount, "Size Match?");
		 * writeToCell(row, ++colCount, "Brand Match?"); writeToCell(row, ++colCount,
		 * "Review?"); }
		 * 
		 * 
		 * private void createItemRow(HSSFRow row, ScrapeItemDTO ssItem, ScrapeItemDTO
		 * wegItem, int mapCount){ int colCount = 0; writeToCell(row, colCount,
		 * ssItem.dept); writeToCell(row, ++colCount, ssItem.dept); writeToCell(row,
		 * ++colCount, ssItem.itemName + " - " + ssItem.dblSize + " " +
		 * ssItem.itemUOMDesc); writeToCell(row, ++colCount, wegItem.itemName + " - " +
		 * wegItem.dblSize + " " + wegItem.itemUOMDesc); writeToCell(row, ++colCount,
		 * "NA"); writeToCell(row, ++colCount, ssItem.UPC); writeToCell(row, ++colCount,
		 * wegItem.wegmansProductCode); writeToCell(row, ++colCount,
		 * wegItem.shoppingURL); writeToCell(row, ++colCount, ssItem.minPrice +"," +
		 * ssItem.maxPrice +"," +ssItem.modePrice); writeToCell(row, ++colCount,
		 * wegItem.regPrice); writeToCell(row, ++colCount, wegItem.salePrice);
		 * writeToCell(row, ++colCount, Integer.toString(wegItem.score)); int pctScore =
		 * 0; if( ssItem.itemWords.length > 0) pctScore =
		 * wegItem.score*100/ssItem.itemWords.length;
		 * 
		 * writeToCell(row, ++colCount, Integer.toString(wegItem.pctScore)+ "%");
		 * writeToCell(row, ++colCount, wegItem.priceMatch? "Y":"N"); writeToCell(row,
		 * ++colCount, wegItem.sizeMatch? "Y":"N"); writeToCell(row, ++colCount,
		 * wegItem.brandMatch? "Y":"N"); if( wegItem.priceMatch && wegItem.sizeMatch &&
		 * wegItem.brandMatch && wegItem.score >= 3 && (mapCount == 1)) writeToCell(row,
		 * ++colCount, "N"); else writeToCell(row, ++colCount, "Y"); }
		 * 
		 * private void writeToCell( HSSFRow row, int colCount, String val) {
		 * scrapeUtil.writeToCell( row, colCount, val); }
		 * 
		 * private ArrayList<ScrapeItemDTO> filterBestMatch(ScrapeItemDTO ssItem,
		 * ArrayList<ScrapeItemDTO> inpMappedItemsList){
		 * 
		 * //After first level of filtering, removing sizes that are drastically
		 * different // for the same UOM ArrayList<ScrapeItemDTO> mappedItemsList = new
		 * ArrayList<ScrapeItemDTO> (); for( ScrapeItemDTO item: inpMappedItemsList){
		 * if( item.sizeMatch ) mappedItemsList.add(item); else if (
		 * ssItem.itemUOMDesc.equalsIgnoreCase(item.itemUOMDesc)){ if(
		 * Math.abs(ssItem.dblSize - item.dblSize)/ssItem.dblSize <= 0.05)
		 * mappedItemsList.add(item); } else{ //If the UOMs are different, add them
		 * mappedItemsList.add(item); }
		 * 
		 * }
		 * 
		 * //If mandatory word is present, then eliminate items that don't have it
		 * boolean reservedWordPresent = false; String reservedWord=""; for( int i=0; i
		 * <ssItem.itemWords.length; i++){ if( ssItem.itemWords[i].length()> 1 &&
		 * ssItem.itemWords[i].charAt(0)=='['){ reservedWordPresent = true; reservedWord
		 * = ssItem.itemWords[i].substring(1); break; } } if( reservedWordPresent){
		 * ArrayList<ScrapeItemDTO> bestMatchList = new ArrayList<ScrapeItemDTO> ();
		 * for( ScrapeItemDTO item: mappedItemsList){ for( int i=0; i
		 * <item.itemWords.length; i++){ if(
		 * item.itemWords[i].equalsIgnoreCase(reservedWord)){ bestMatchList.add(item);
		 * break; } } } mappedItemsList = bestMatchList; }
		 * 
		 * int priceHighestScore = 0; int sizePriceHighestScore = 0; boolean
		 * priceMatchPresent = false; boolean sizePriceMatchPresent = false; for(
		 * ScrapeItemDTO item: mappedItemsList){ if (item.priceMatch && item.score >
		 * priceHighestScore){ priceMatchPresent = true; priceHighestScore = item.score;
		 * } if (item.priceMatch && item.sizeMatch && item.score >
		 * sizePriceHighestScore){ sizePriceMatchPresent = true; sizePriceHighestScore =
		 * item.score; } }
		 * 
		 * if( priceMatchPresent){ ArrayList<ScrapeItemDTO> bestMatchList = new
		 * ArrayList<ScrapeItemDTO> (); for( ScrapeItemDTO item: mappedItemsList){
		 * 
		 * if(sizePriceMatchPresent){ if( item.score == sizePriceHighestScore &&
		 * item.sizeMatch && item.priceMatch) bestMatchList.add(item); } else {//Size
		 * does not match, Price matches if( item.score == priceHighestScore &&
		 * item.priceMatch) bestMatchList.add(item); }
		 * 
		 * } mappedItemsList = bestMatchList; }
		 * 
		 * // Filter only the items with Brand match //if the list > 1, remove any non
		 * brand match items if they are present with brand match items boolean
		 * brandMatchPresent = false; for( ScrapeItemDTO item: mappedItemsList){ if(
		 * item.brandMatch){ brandMatchPresent = true; break; } } if(
		 * brandMatchPresent){ ArrayList<ScrapeItemDTO> bestMatchList = new
		 * ArrayList<ScrapeItemDTO> (); for( ScrapeItemDTO item: mappedItemsList){
		 * if(item.brandMatch){ bestMatchList.add(item); } } mappedItemsList =
		 * bestMatchList; }
		 * 
		 * 
		 * //If the list size in > 1, pick the best score int bestPctScore = 0; for(
		 * ScrapeItemDTO item: mappedItemsList){ if( item.pctScore > bestPctScore)
		 * bestPctScore = item.pctScore; }
		 * 
		 * ArrayList<ScrapeItemDTO> bestMatchList = new ArrayList<ScrapeItemDTO> ();
		 * for( ScrapeItemDTO item: mappedItemsList){ if(item.pctScore == bestPctScore){
		 * bestMatchList.add(item); } } mappedItemsList = bestMatchList;
		 * 
		 * int minWegWords = 50; for( ScrapeItemDTO item: mappedItemsList){ if(
		 * item.itemWords.length < minWegWords) minWegWords = item.itemWords.length; }
		 * 
		 * //if Multiple best scores, pick the one with the shortest words bestMatchList
		 * = new ArrayList<ScrapeItemDTO> (); for( ScrapeItemDTO item: mappedItemsList){
		 * if(item.itemWords.length== minWegWords){ bestMatchList.add(item); } }
		 * mappedItemsList = bestMatchList;
		 * 
		 * return mappedItemsList;
		 * 
		 * 
		 * } }
		 */