package com.pristine.webscrape.Wegmans;
 /* 
 * import java.io.FileOutputStream; import java.io.IOException; import
 * java.util.ArrayList; import java.util.HashMap; import java.util.List;
 * 
 * import org.apache.log4j.Logger; import org.apache.poi.hssf.usermodel.HSSFRow;
 * import org.apache.poi.hssf.usermodel.HSSFSheet; import
 * org.apache.poi.hssf.usermodel.HSSFWorkbook;
 * 
 * import com.gargoylesoftware.htmlunit.WebClient; import
 * com.gargoylesoftware.htmlunit.html.HtmlPage; import
 * com.gargoylesoftware.htmlunit.html.HtmlTableRow;
 */
 public class WegmansProductCodeScrape {
 /* 
 * private static Logger logger = Logger.getLogger("WGMProductCodeScraping");
 * WegmansWebScrapeUtil scrapeUtil = new WegmansWebScrapeUtil();
 * 
 * // Allentown PA storeId": "79" // Harrisburg "storeId": "45" -- Silver
 * Springs??
 * 
 * 
 * HashMap <String, String> categoryMap = new HashMap <String, String> ();
 *//**
	 * @param args
	 *//*
		 * public static void main(String[] args) { // TODO Auto-generated method stub
		 * WegmansProductCodeScrape productCodeScrape = new WegmansProductCodeScrape();
		 * productCodeScrape.doInit();
		 * 
		 * if( args.length < 2){
		 * logger.info("Invalid argument: WegmansProductCodeScrape StoreId department");
		 * System.exit(1); } String userId = null; String password = null; if(
		 * args.length == 4){ userId = args[2]; password = args[3];
		 * 
		 * }
		 * 
		 * try{ logger.info("Wegmans Produce Code Scraping started ...");
		 * 
		 * productCodeScrape.startProcess(args[0], args[1], userId, password);
		 * logger.info("Wegmans Produce Code Scraping Done ..."); }catch (Exception e){
		 * logger.error("Exception in Product scraping", e); }
		 * 
		 * }
		 * 
		 * private void startProcess ( String strId, String deptName, String userId,
		 * String password) throws Exception {
		 * 
		 * if( !categoryMap.containsKey(deptName) ){
		 * logger.info("Invalid department name"); System.exit(1); } final WebClient
		 * webClient = scrapeUtil.storeLogin(strId, userId, password);
		 * 
		 * //Get the URL
		 * 
		 * int MAX_HITS = 20; int ITEMS_PER_PAGE = 24; boolean hasNextPage = true; int
		 * hitCounter = 0; String categoryId = categoryMap.get(deptName);
		 * //Ns=P_Velocity%7c1 String baseMenuURL =
		 * "http://www.wegmans.com/webapp/wcs/stores/servlet/ProductListView?Ne=6&langId=-1&storeId=10052&catalogId=10002&Ns=P_FacebookLikes|1||P_Velocity|1||P_Name|0&N="
		 * +categoryId;
		 * 
		 * ArrayList<ScrapeItemDTO> wegmanItemListArray = new ArrayList<ScrapeItemDTO>
		 * (); do{
		 * 
		 * 
		 * 
		 * 
		 * WegmansMapDTO item = new WegmansMapDTO(); item.wegmansProductCode = "661144";
		 * scrapeUtil.scrapeItembyProductCode(webClient, item); if( MAX_HITS > 0) break;
		 * 
		 * 
		 * String shoppingMenuURL = baseMenuURL + "&Nao=" + hitCounter *ITEMS_PER_PAGE;
		 * //execute the URL HtmlPage page = webClient.getPage(shoppingMenuURL);
		 * //String response = page.asText(); List<HtmlTableRow> tblRowList =
		 * scrapeUtil.getItemListTable(page);
		 * 
		 * scrapeUtil.setReferenceData(deptName, shoppingMenuURL,true); //
		 * scrapeUtil.processItemTableList(webClient,tblRowList, wegmanItemListArray);
		 * 
		 * //Write function to see if next exists hitCounter++; hasNextPage =
		 * scrapeUtil.hasNextPageLink(page); if( hasNextPage ){
		 * logger.info("Next Page Link Exists, Hit Counter = " + hitCounter); } else{
		 * logger.info("Next Page Link Does Not Exists, Hit Counter = " + hitCounter); }
		 * }while(hasNextPage && hitCounter < MAX_HITS);
		 * 
		 * 
		 * if( wegmanItemListArray.size() > 0 ){
		 * scrapeUtil.scrapeItembyProductCode(webClient, wegmanItemListArray.get(0)); }
		 * 
		 * writeOutputToExcel("C:/Presto/SS_ProdScrape/Wegman_Map_"+deptName+".xls",
		 * wegmanItemListArray);
		 * //writeOutputToExcel("C:/Presto/Wegman_Map_"+deptName+".xls",
		 * wegmanItemListArray); //If there are more items, }
		 * 
		 * 
		 * private void writeOutputToExcel(String fileName, ArrayList<ScrapeItemDTO>
		 * wegmanItemList) throws IOException {
		 * 
		 * HSSFWorkbook wb = new HSSFWorkbook(); int rowCount = 0; HSSFSheet sheet =
		 * wb.createSheet(); HSSFRow row = sheet.createRow(rowCount);
		 * createHeaderRow(row); int count = 0; for ( ScrapeItemDTO item
		 * :wegmanItemList){ rowCount++; row = sheet.createRow(rowCount);
		 * createItemRow(row, item); count++; //if( count == MAX_ITEM_COUNT) break; }
		 * 
		 * FileOutputStream fileOut = new FileOutputStream(fileName); wb.write(fileOut);
		 * fileOut.close(); }
		 * 
		 * private void createHeaderRow(HSSFRow row) { int colCount = 0;
		 * writeToCell(row, colCount, "Dept"); writeToCell(row, ++colCount,
		 * "Wegman's Item Name"); writeToCell(row, ++colCount, "Size"); writeToCell(row,
		 * ++colCount, "UOM"); writeToCell(row, ++colCount, "Reg Rtl"); writeToCell(row,
		 * ++colCount, "Promo Rtl"); writeToCell(row, ++colCount, "Product Code");
		 * writeToCell(row, ++colCount, "UPC"); writeToCell(row, ++colCount, "URL");
		 * 
		 * }
		 * 
		 * 
		 * private void createItemRow(HSSFRow row, ScrapeItemDTO item){ int colCount =
		 * 0; writeToCell(row, colCount, item.dept); writeToCell(row, ++colCount,
		 * item.itemName); writeToCell(row, ++colCount, item.itemSize); writeToCell(row,
		 * ++colCount, item.itemUOMDesc); writeToCell(row, ++colCount, item.regPrice);
		 * writeToCell(row, ++colCount, item.salePrice); writeToCell(row, ++colCount,
		 * "'"+item.wegmansProductCode); writeToCell(row, ++colCount, "'"+item.UPC);
		 * writeToCell(row, ++colCount, item.shoppingURL);
		 * 
		 * }
		 * 
		 * private void writeToCell( HSSFRow row, int colCount, String val) {
		 * scrapeUtil.writeToCell( row, colCount, val); }
		 * 
		 * private void doInit(){
		 * 
		 * categoryMap.put("TestReview", "207+4294822582+235+4294820928");
		 * 
		 * categoryMap.put("baby", "207+4294967107"); categoryMap.put("bakery",
		 * "207+4294966991"); categoryMap.put("baking", "207+4294795877");
		 * categoryMap.put("beverages", "207+4294967111"); categoryMap.put("bulk-foods",
		 * "207+4294966963"); categoryMap.put("catering-partytrays", "207+4294822578");
		 * categoryMap.put("cheese-shop", "207+4294966634");
		 * categoryMap.put("complements-kitchen-dining", "207+4294822581");
		 * categoryMap.put("cosmetics", "207+4294967071"); categoryMap.put("dairy",
		 * "207+4294967100"); categoryMap.put("deli", "207+4294966344");
		 * categoryMap.put("floral", "207+4294966305"); categoryMap.put("frozen",
		 * "207+4294966994"); categoryMap.put("gifts-party-supplies", "207+4294802631");
		 * categoryMap.put("grocery", "207+4294967131");
		 * categoryMap.put("health-wellness", "207+4294822582");
		 * categoryMap.put("household-supplies", "207+4294967121");
		 * categoryMap.put("international-foods", "207+4294808684");
		 * categoryMap.put("kosher", "207+4294967076"); categoryMap.put("laundry",
		 * "207+4294822583"); categoryMap.put("meat", "207+4294966749");
		 * categoryMap.put("nature-marketplace", "207+4294967125");
		 * categoryMap.put("office-supplies", "207+4294802674");
		 * categoryMap.put("paper-plastic", "207+4294822584");
		 * categoryMap.put("personal-care", "207+4294967138");
		 * categoryMap.put("pet-foods", "207+4294820948");
		 * categoryMap.put("prepared-foods", "207+429496649");
		 * categoryMap.put("produce", "207+4294967010"); categoryMap.put("sea-food",
		 * "207+429496675"); categoryMap.put("seasonal", "207+4294966682");
		 * categoryMap.put("spirits", "207+4294783417"); categoryMap.put("wine",
		 * "207+4294783170");
		 * 
		 * //Week5 list categoryMap.put("eye-ear", "207+4294967138+4294820915");
		 * categoryMap.put("FOOT-CARE", "207+4294822582+4294966975");
		 * categoryMap.put("COUGH-COLD","207+4294822582+4294820920");
		 * categoryMap.put("PAIN-REMEDIES","207+4294822582+4294967026");
		 * categoryMap.put("NUTRITION", "207+4294822582+4294966469");
		 * categoryMap.put("DIET-AIDS", "207+4294822582+4294966909");
		 * categoryMap.put("NUTRITION_BARS1","207+4294967125+4294967106+4294966434");
		 * //Natures
		 * categoryMap.put("NUTRITION_BARS2","207+4294967125+4294967106+4294966354");//
		 * Natures
		 * categoryMap.put("NUTRITION_BARS3","207+4294967125+4294967106+4294966590");//
		 * Natures
		 * 
		 * categoryMap.put("BATTERIES", "207+4294967121+4294967123");
		 * categoryMap.put("FRESHENERS", "207+4294822583+4294820905");
		 * categoryMap.put("LIGHT-BULBS", "207+4294967121+4294795017");
		 * categoryMap.put("FROZEN-JUICES", "207+4294966994+4294820937");
		 * 
		 * categoryMap.put("FROZEN-JUICES2", "207+4294966994+4294861964");
		 * 
		 * categoryMap.put("SEAFOOD-CANNED1", "207+4294967131+4294789089");
		 * 
		 * categoryMap.put("SEAFOOD-CANNED2", "207+4294967131+4294820921");
		 * categoryMap.put("FRUITS-CANNED", "207+4294967131+4294966944");
		 * categoryMap.put("JAMS-JELLIES-PNUTBTR", "207+4294967131+4294820939");
		 * categoryMap.put("SUGAR", "207+4294795877+4294795860");
		 * categoryMap.put("GRATED-CHEESE", "207+4294967100+4294967102");
		 * categoryMap.put("LIGHT-BULBS", "207+4294967121+4294795017");
		 * 
		 * categoryMap.put("BEVERAGE_WATER", "207+4294967111+4294966858");
		 * categoryMap.put("PAPER-PLASTIC", "207+4294822584");
		 * categoryMap.put("DESSERTS-GEL", "207+4294795877+4294795875");
		 * categoryMap.put("DESSERTS-GEL2", "207+4294967100+4294966930");
		 * categoryMap.put("BAKED-GOODS-FROZEN","207+4294966994+4294966930");
		 * categoryMap.put("REFRIG-MEAT","207+4294966749");
		 * 
		 * //Week 6 List categoryMap.put("first-aid", "207+4294822582+4294966875");
		 * categoryMap.put("stomach", "207+4294822582+4294966937");
		 * categoryMap.put("FEMININE-HYGIENE", "207+4294967138+4294820917");
		 * categoryMap.put("SANITARY-PROTECTION", "207+4294967138+4294967030");
		 * categoryMap.put("Incontient", "207+4294967138+4294966797");
		 * categoryMap.put("Smoking-Cessation", "207+4294822582+4294966978");
		 * categoryMap.put("INTIMATE-CARE","207+4294967138+4294966911");
		 * 
		 * categoryMap.put("BREAD-DOUGH-FROZEN", "207+4294966994+4294966649");
		 * categoryMap.put("BREAD-DOUGH-FROZEN2", "207+4294966994+4294966121");
		 * categoryMap.put("FROZEN-FRUITS", "207+4294966994+4294966944");
		 * categoryMap.put("RICE-CAKES","207+4294967131+4294820973+4294966453");
		 * categoryMap.put("REFRIGERATED-CONDIMENTS","207+4294967100+4294966071");
		 * categoryMap.put("REFRIGERATED-DIPS","207+4294967100+4294966659");
		 * categoryMap.put("PICKLES-OLIVES","207+4294967131+4294808657");
		 * categoryMap.put("SOUPS", "207+4294967131+4294967056");
		 * categoryMap.put("FLAVORED-SYRUP","207+4294967111+4294810562");
		 * categoryMap.put("CONDIMENTS-SAUCES", "207+4294967131+4294808688");
		 * categoryMap.put("laundry", "207+4294822583");
		 * categoryMap.put("MEXICAN-PRODUCTS","207+4294808684+4294808681");
		 * categoryMap.put("PACKAGED-MILK", "207+4294795877+4294802114");
		 * categoryMap.put("SALAD-MAYO-DRESSING","207+4294967131+4294808674");
		 * 
		 * categoryMap.put("SALAD-TOPPINGS", "207+4294967131+4294813586");
		 * categoryMap.put("KETCHUP-MUSTARD", "207+4294967131+4294808675");
		 * categoryMap.put("KOSHER", "207+4294967076");
		 * 
		 * 
		 * categoryMap.put("KOSHER-REFRIG","207+4294967076+4294967058"); //Week 1 List
		 * categoryMap.put("vitamins", "207+4294822582+4294820928");
		 * 
		 * 
		 * 
		 * //Week 2 list
		 * 
		 * categoryMap.put("TEA", "207+4294967111+4294967082"); categoryMap.put("TEA2",
		 * "207+4294967125+4294967061+4294965495"); //Organic categoryMap.put("TEA3",
		 * "207+4294967125+4294967061+4294967081");//Organic categoryMap.put("TEA4",
		 * "207+4294967125+4294967061+4294862236"); //Organic
		 * categoryMap.put("FRESH-BREAD", "207+4294966991+4294966628");
		 * categoryMap.put("FRESH-BREAD2","207+4294966991+4294824971");
		 * categoryMap.put("FRESH-BREAD3","207+4294966991+4294966815"); //Rolls
		 * categoryMap.put("FRESH-BREAD4","207+4294966991+4294861827"); //Weight
		 * Watchers Bread categoryMap.put("FRESH-BREAD5","207+4294966991+4294966987");
		 * //Thomas
		 * 
		 * 
		 * 
		 * 
		 * 
		 * categoryMap.put("YEAST-REFRIG","207+4294967100+4294966624");
		 * 
		 * 
		 * 
		 * categoryMap.put("FAMILY-PLANNING", "207+4294967138+4294966911");
		 * categoryMap.put("FAMILY-PLANNING2", "207+4294967138+4294967030+4294965935");
		 * 
		 * 
		 * 
		 * //Week 3 categoryMap.put("DRY-MIXES", "207+4294967131+4294820955");
		 * categoryMap.put("STUFFING", "207+4294966991+4294966506");
		 * 
		 * 
		 * categoryMap.put("FROZEN-PASTA", "207+4294966994+4294760404");
		 * 
		 * 
		 * categoryMap.put("WHIPPED-TOPPING","207+4294967100+4294966832");
		 * 
		 * categoryMap.put("SHOE-POLISH", "207+4294822583+4294967000+4294966851");
		 * 
		 * 
		 * 
		 * 
		 * //Week 4
		 * 
		 * 
		 * categoryMap.put("DRINK-MIXES", "207+4294967111+4294966552");
		 * 
		 * categoryMap.put("POPCORN","207+4294967131+4294820973+4294966956");
		 * categoryMap.put("POPCORN2","207+4294967131+4294820973+4294819888");
		 * categoryMap.put("CHIPS-SNACKS","207+4294967131+4294820973"); //big
		 * categoryMap.put("BAKERY-SNACKS","207+4294966991+4294966993");
		 * 
		 * 
		 * categoryMap.put("PAPER-PRODUCTS","207+4294822584");
		 * 
		 * categoryMap.put("ETHINIC-FOODS","207+4294808684");
		 * 
		 * categoryMap.put("ICE-CUBES", "207+4294966994+4294825268");
		 * 
		 * 
		 * 
		 * categoryMap.put("SEASONAL", "207+4294966682");
		 * 
		 * //categoryMap.put("","");
		 * 
		 * 
		 * 
		 * //Week 1 - Feb 1 categoryMap.put("BABY-FOOD", "207+4294967107+4294820916");
		 * categoryMap.put("INFANT-FORMULA", "207+4294967107+4294818309");
		 * categoryMap.put("baking", "207+4294795877"); categoryMap.put("baking2",
		 * "207+4294967125+4294967092");//Nature's categoryMap.put("TOMATOES-CANNED",
		 * "207+4294967131+4294811112"); categoryMap.put("VEGETABLES-CANNED",
		 * "207+4294967131+4294967097");
		 * 
		 * categoryMap.put("BREAKFAST-FOOD-FROZEN", "207+4294966994+4294966987");
		 * categoryMap.put("BREAKFAST-FOOD-FROZEN2",
		 * "207+4294966994+4294820941+4294861932");
		 * 
		 * categoryMap.put("HONEY", "207+4294967131+4294820939");
		 * 
		 * categoryMap.put("BUTTER-SPREADS", "207+4294967100+4294820960");
		 * 
		 * categoryMap.put("EGG-SUBSTITUTES", "207+4294967100+4294820904");
		 * categoryMap.put("MILK", "207+4294967100+4294802114");
		 * categoryMap.put("COFFEE", "207+4294967111+4294966767");
		 * 
		 * categoryMap.put("COCOA", "207+4294967111+4294810562");
		 * 
		 * categoryMap.put("DISH-DETERGENTS", "207+4294822583+4294967136");
		 * categoryMap.put("DISH-DETERGENTS2", "207+4294967125+4294820894+4294967135");
		 * 
		 * categoryMap.put("COOKIES", "207+4294967131+4294967104");
		 * categoryMap.put("CRACKERS", "207+4294967131+4294966905");
		 * categoryMap.put("DOG-FOOD", "207+4294820948+4294820929");
		 * 
		 * 
		 * categoryMap.put("SHAMPOO", "207+4294967138+4294820976");
		 * categoryMap.put("HAIR-CARE", "207+4294967138+4294967120");
		 * categoryMap.put("HAIR-COLOR", "207+4294967138+4294966885");
		 * categoryMap.put("HAIR-STYLE", "207+4294967138+4294967006");
		 * categoryMap.put("HAIR-ACCESSORIES", "207+4294967138+4294967018");
		 * 
		 * 
		 * categoryMap.put("FROZEN-SNACKS", "207+4294966994+4294966017");
		 * categoryMap.put("ICECREAM", "207+4294966994+4294820935");
		 * 
		 * categoryMap.put("BAKED-GOODS-FROZEN", "207+4294966994+4294966930");
		 * categoryMap.put("BAKED-GOODS-FROZEN2", "207+4294966991+4294861827");
		 * categoryMap.put("FLAVORED-SYRUP","207+4294967111+4294810562");
		 * 
		 * categoryMap.put("MEAT-SUBS","207+4294966749+4294966313"); //Meat Substitutes
		 * categoryMap.put("MEAT-SUBS2","207+4294966994+4294820941+4294966263");
		 * 
		 * categoryMap.put("PAPER-TOWELS","207+4294822584+4294966642");
		 * categoryMap.put("SYRUPS","207+4294967131+4294966987+4294966593");
		 * 
		 * categoryMap.put("REFRIGE-BAKED-GOODS","207+4294967100+4294966930");
		 * 
		 * //Week 2 - Feb 6 categoryMap.put("LAUNDRY", "207+4294822583");
		 * categoryMap.put("LAUNDRY2", "207+4294967125+4294820894"); //Organic
		 * categoryMap.put("MILK2", "207+4294967100+4294966112"); //Lactose Free
		 * categoryMap.put("MILK3", "207+4294967125+4294967080+4294966787"); //Organic
		 * categoryMap.put("MILK4", "207+4294967080+4294966572"); //Soy
		 * categoryMap.put("beer", "207+4294795881");
		 * categoryMap.put("YOGURT","207+4294967100+4294966758");
		 * categoryMap.put("FROZEN-NOVELTIES", "207+4294966994+4294966897");
		 * 
		 * categoryMap.put("BABY-WIPES", "207+4294967107+4294966999");
		 * categoryMap.put("DIAPERS", ""); //Same as wipes
		 * categoryMap.put("BATH-TISSUE", "207+4294822584+4294966477");
		 * categoryMap.put("CAT-FOOD", "207+4294820948+4294820949");
		 * categoryMap.put("FROZEN-WHIPPED-TOPPING", "207+4294966994+4294966832");
		 * categoryMap.put("FLOUR", "207+4294795877+4294795874");
		 * categoryMap.put("JUICES-SHELF-STABLE", "207+4294967111+4294966980");
		 * categoryMap.put("ICE-CREAM-MISC", "207+4294966994+4294820892"); //Toppings,
		 * Cones categoryMap.put("GRAVY-SAUCES", "207+4294967131+4294820951");
		 * categoryMap.put("RICE", "207+4294967131+4294820955");
		 * categoryMap.put("CONDIMENTS-SAUCES", "207+4294967131+4294808688");
		 * 
		 * categoryMap.put("JUICES-REFRIGE", "207+4294967100+4294820937");
		 * categoryMap.put("JELLO1", "207+4294795877+4294795875");
		 * categoryMap.put("JELLO2", "207+4294967100+4294966930");
		 * categoryMap.put("BEANS-CANNED", "207+4294967131+4294967097");
		 * 
		 * categoryMap.put("DEODORANT", "207+4294967138+4294820917");
		 * categoryMap.put("NUTRITION", "207+4294822582+4294966469");
		 * categoryMap.put("DIET-AIDS", "207+4294822582+4294966909");
		 * 
		 * //Week 3 - //Feb 13
		 * 
		 * categoryMap.put("PASTA", "207+4294967131+4294858812");
		 * categoryMap.put("PASTA-SAUCE", "207+4294967131+4294811113");
		 * categoryMap.put("SNACK-NUTS", "207+4294967131+4294820973+4294966556");
		 * categoryMap.put("SNACK-NUTS2",
		 * "207+4294967131+4294820973+4294811075");//Peanuts
		 * categoryMap.put("SNACK-NUTS3", "207+4294967131+4294820973+4294811078");
		 * //Almonds categoryMap.put("SNACK-NUTS4",
		 * "207+4294967131+4294820973+4294811074"); //Cashew
		 * categoryMap.put("WHOLESOME_SNACKS1", "207+4294967131+4294966987+4294966789");
		 * categoryMap.put("WHOLESOME_SNACKS2", "207+4294967131+4294966987+4294966912");
		 * categoryMap.put("WHOLESOME_SNACKS3", "207+4294967125+4294967106+4294966354");
		 * categoryMap.put("BREAKFAST", "207+4294967131+4294966987");
		 * categoryMap.put("BREAKFAST2", "207+4294967125+4294967106+4294967021");
		 * //Organic categoryMap.put("BREAKFAST3",
		 * "207+4294967125+4294967106+4294966354"); //Weight Watchers
		 * 
		 * categoryMap.put("CHEESE","207+4294967100+4294967102");
		 * categoryMap.put("CREAM","207+4294967100+4294820902");
		 * categoryMap.put("SOURCREAM","207+4294967100+4294966779");
		 * categoryMap.put("COTTAGE-CHEESE","207+4294967100+4294820919");
		 * categoryMap.put("CREAM-CHEESE","207+4294967100+4294966737");
		 * categoryMap.put("FROZEN-VEGETABLES", "207+4294966994+4294967097");
		 * 
		 * categoryMap.put("HOUSEHOLD-CLEANING", "207+4294822583+4294965783");
		 * categoryMap.put("HOUSEHOLD-SUPPLIES","207+4294967121");
		 * categoryMap.put("NON-CARB-BEVERAGES","207+4294967111+4294967082");
		 * categoryMap.put("NON-CARB-BEVERAGES2","207+4294967111+4294820936");
		 * categoryMap.put("NON-CARB-BEVERAGES3","207+4294967111+4294966767");
		 * categoryMap.put("NON-CARB-BEVERAGES4","207+4294967111+4294966980");
		 * categoryMap.put("SHORTENING-OIL","207+4294967131+4294820959");
		 * categoryMap.put("VINEGAR","207+4294967131+4294808677");
		 * categoryMap.put("BATHROOM-CLEANERS","207+4294822583+4294790109");
		 * categoryMap.put("CAT-LITTER","207+4294820948+4294820949+4294966643");
		 * 
		 * categoryMap.put("FACIAL-TISSUE","207+4294822584+4294966383");
		 * categoryMap.put("MENS-SHAVING","207+4294967138+4294966983");
		 * categoryMap.put("FACIAL-SKIN-CARE","207+4294967138+4294967062");
		 * categoryMap.put("SKIN-CARE","207+4294967138+4294820918");
		 * categoryMap.put("SUNCARE","207+4294967138+4294967140");
		 * categoryMap.put("BABY-NEEDS","207+4294967107+4294820903");
		 * categoryMap.put("BABY-ACCESSORIES","207+4294967107+4294967090");
		 * categoryMap.put("BABY-ACCESSORIES2","207+4294967107+4294966620");
		 * categoryMap.put("FROZEN-FOODS", "207+4294966994+4294820941");
		 * 
		 * //Week 4 - Feb 20 categoryMap.put("BEANS-CANNED",
		 * "207+4294967131+4294967097");
		 * categoryMap.put("PREP-FOODS-READY-TO-SERVE","207+4294967131+4294820955");
		 * categoryMap.put("CARBONATED-BEVERAGES", "207+4294967111+4294966969");
		 * categoryMap.put("ISOTONICS", "207+4294967111+4294820936");
		 * categoryMap.put("POWDERED-SOFT-DRINKS", "207+4294967111+4294966552");
		 * categoryMap.put("TONIC-WATER", "207+4294967111+4294966858");
		 * categoryMap.put("CANDY-GUM", "207+4294967131+4294820974");
		 * categoryMap.put("SNACKS", "207+4294967131+4294820973");
		 * categoryMap.put("MEATS-CANNED","207+4294967131+4294820921");
		 * categoryMap.put("MEATS-CANNED2","207+4294967131+4294823822");//Star Kist
		 * Brand
		 * 
		 * categoryMap.put("PAPER-NAPKINS", "207+4294822584+4294966615");
		 * categoryMap.put("TABLE-TOP", "207+4294822584+4294798619");
		 * categoryMap.put("BUTTER-SPREADS", "207+4294967100+4294820960");
		 * categoryMap.put("EGG-SUBSTITUTES", "207+4294967100+4294820904");
		 * categoryMap.put("FROZEN-PIZZA", "207+4294966994+4294966142");
		 * categoryMap.put("FROZEN-SNACKS", "207+4294966994+4294966017");
		 * categoryMap.put("FROZEN-FRIES","207+4294966994+4294820940");
		 * categoryMap.put("FROZEN-SEA-FOOD","207+4294966994+4294966864");
		 * categoryMap.put("FROZEN-MEAT","207+4294966994+4294764108");
		 * 
		 * categoryMap.put("ORAL-HYGIENE", "207+4294967138+4294966954");
		 * categoryMap.put("SOAP", "207+4294967138+4294966650");
		 * 
		 * }
		 */
		  }
		 //*/