
 package com.pristine.webscrape.Wegmans;
 /* 
 * import java.io.IOException; import java.net.MalformedURLException; import
 * java.util.ArrayList; import java.util.Iterator; import java.util.List;
 * 
 * import org.apache.commons.httpclient.Credentials; import
 * org.apache.commons.httpclient.UsernamePasswordCredentials; import
 * org.apache.commons.httpclient.auth.AuthScheme; import
 * org.apache.commons.httpclient.auth.CredentialsProvider; import
 * org.apache.log4j.Logger; import org.apache.poi.hssf.usermodel.HSSFCell;
 * import org.apache.poi.hssf.usermodel.HSSFRow; import
 * org.w3c.dom.NamedNodeMap; import org.w3c.dom.Node;
 * 
 * import com.gargoylesoftware.htmlunit.BrowserVersion; import
 * com.gargoylesoftware.htmlunit.DefaultCredentialsProvider; import
 * com.gargoylesoftware.htmlunit.ProxyConfig; import
 * com.gargoylesoftware.htmlunit.WebClient; import
 * com.gargoylesoftware.htmlunit.html.HtmlElement; import
 * com.gargoylesoftware.htmlunit.html.HtmlPage; import
 * com.gargoylesoftware.htmlunit.html.HtmlTable; import
 * com.gargoylesoftware.htmlunit.html.HtmlTableCell; import
 * com.gargoylesoftware.htmlunit.html.HtmlTableRow;
 */
 public class WegmansWebScrapeUtil { 
 /* private static Logger logger =
 * Logger.getLogger("WGMWebScrapingUtil");
 * 
 * private String currentDeptName; private String currentURL; boolean
 * processSale = false; public void setReferenceData(String departmentName,
 * String urlListing, boolean saleProcessFlg){ currentDeptName = departmentName;
 * currentURL = urlListing; processSale = saleProcessFlg;
 * 
 * } public WebClient storeLogin(String strId, String userId, String password)
 * throws IOException, MalformedURLException { final WebClient webClient = new
 * WebClient(BrowserVersion.INTERNET_EXPLORER_8);
 * 
 * webClient.setCssEnabled(false); webClient.setJavaScriptEnabled(false); String
 * urlStr =
 * "http://www.wegmans.com/webapp/wcs/stores/servlet/WegSetStorePreferenceCmd?storeId=10052&catalogId=10002&langId=-1&URL=RedirectView&redirecturl=StoreLocatorView&showSC=no&preferredStore="
 * + strId; boolean useProxy = false; if( userId == null || password == null){
 * useProxy = false; }else{ useProxy = true; }
 * 
 * if( useProxy){ String proxyserver = "proxy.sands.aholdusa.com"; String port =
 * "8080"; int portno = Integer.parseInt(port); ProxyConfig pConfig = new
 * ProxyConfig(proxyserver, portno); webClient.setProxyConfig(pConfig);
 * 
 * Credentials c = new UsernamePasswordCredentials("guest","guest");
 * 
 * //AuthScheme as = new DefaultAuthScheme(); //as.
 * 
 * //webClient.set CredentialsProvider credentialsProvider ;
 * 
 * webClient.setCredentialsProvider(credentialsProvider)
 * 
 * 
 * logger.info("Logging into Proxy server "); DefaultCredentialsProvider creds =
 * new DefaultCredentialsProvider(); creds.addCredentials(userId, password);
 * webClient.setCredentialsProvider(creds); }
 * 
 * // HtmlPage page = // webClient.getPage(
 * "http://www.wegmans.com/webapp/wcs/stores/servlet/WegSetStorePreferenceCmd?storeId=10052&catalogId=10002&langId=-1&URL=RedirectView&redirecturl=StoreLocatorView&preferredStore=32&showSC=no"
 * ); HtmlPage page = webClient.getPage(urlStr);
 * 
 * 
 * logger.info("Successful connecting ...."); //page = webClient.getPage(
 * "http://www.wegmans.com/webapp/wcs/stores/servlet/WegSetSessionVariableCmd?property=VIEWRECS&value=50"
 * );
 * 
 * return webClient;
 * 
 * }
 * 
 * public void printPagition(HtmlPage page) { if( page != null){ List<?> elmList
 * = page.getByXPath("//table[@id='pagination']"); HtmlTable ht = (HtmlTable)
 * elmList.get(0); logger.info(ht.asXml()); }
 * 
 * }
 * 
 * public List<HtmlTableRow> getItemListTable(HtmlPage page) {
 * List<HtmlTableRow> tblRowList = null;
 * 
 * if( page != null){ List<?> elmList =
 * page.getByXPath("//table[@class='shoppinglist-twoCol listing results']");
 * 
 * if (elmList.size() >0){ logger.info("Got Table ...." + elmList.size());
 * HtmlTable ht = (HtmlTable) elmList.get(0);
 * 
 * tblRowList = ht.getRows(); logger.info("Table Row Size is "
 * +tblRowList.size()); //logger.info("Table is " +ht.asText());
 * 
 * } } return tblRowList; }
 * 
 * public void processItemTableList(final WebClient webClient,
 * List<HtmlTableRow> tblRowList, ArrayList<ScrapeItemDTO> wegmanItemListArray)
 * throws IOException, MalformedURLException{
 * 
 * if( tblRowList != null){ //Iterator<?> itr = tblRowList.iterator();
 * 
 * logger.info(tblRowList.size()); for (int i = 1; i < tblRowList.size(); i++){
 * 
 * 
 * For Each row, get Size Price ProductId ProductName ContentURL
 * 
 * 
 * HtmlTableRow tableRow = (HtmlTableRow) tblRowList.get(i); //logger.info(
 * tableRow.asXml());
 * 
 * 
 * String size = ""; String price = ""; String productName = ""; String
 * productId = ""; String upc ="";
 * 
 * List<HtmlTableCell> cells = tableRow.getCells(); Iterator<HtmlTableCell>
 * cellItr = cells.iterator(); while (cellItr.hasNext()){
 * 
 * 
 * HtmlTableCell cell = cellItr.next();
 * 
 * NamedNodeMap nodeNames = cell.getAttributes(); Node nd =
 * nodeNames.getNamedItem("class"); if (nd != null){ if
 * (nd.getNodeValue().equals("size")){ size = cell.asText(); continue; } }
 * 
 * //nd = nodeNames.getNamedItem("class"); if (nd != null){ if
 * (nd.getNodeValue().equals("listprice")){ price = cell.asText(); continue; } }
 * 
 * //nd = nodeNames.getNamedItem("class"); if (nd != null &&
 * nd.getNodeValue().equals("prodImage")){ //logger.info("Node val = " +
 * nd.getNodeValue()); upc = cell.asXml(); int upcEndIndex =
 * upc.indexOf(".jpg");
 * 
 * int beginIndex = upcEndIndex - 13; for ( ; beginIndex >= 0; beginIndex-- ){
 * if( upc.charAt(beginIndex)=='/') break; }
 * 
 * beginIndex = beginIndex+1; if( upcEndIndex > 0 && beginIndex > 0) upc =
 * upc.substring(beginIndex , upcEndIndex);
 * 
 * //logger.info("UPC XML" + upc);
 * 
 * //logger.info("Actual UPC " + upc); continue;
 * 
 * }
 * 
 * 
 * String alignVal =""; nd = nodeNames.getNamedItem("align"); if ( nd != null){
 * alignVal = nd.getNodeValue(); } String valignVal =""; nd =
 * nodeNames.getNamedItem("valign"); if ( nd != null){ valignVal =
 * nd.getNodeValue(); } if( alignVal.equals("left") &&
 * valignVal.equals("middle"))
 * 
 * if (nd != null && nd.getNodeValue().equals("prodName")){ productName =
 * cell.asText(); productName = replaceLineFeed(productName); //get the product
 * ID String s = cell.asXml(); int productIndex = s.indexOf("productId="); if(
 * productIndex >= 0){ productIndex += 10; if (s.length() >= productIndex +6)
 * productId = s.substring(productIndex, productIndex +6); } continue; }
 * 
 * 
 * 
 * 
 * //cell. //logger.info(cell.asXml()); }
 * 
 * 
 * List<?> colList = tableRow.getByXPath("//td[@class='size']"); if(
 * colList.size()>0){ size = ((HtmlElement)colList.get(0)).asText(); }
 * 
 * colList = tableRow.getByXPath("//td[@class='listprice']"); if(
 * colList.size()>0){ price = ((HtmlElement)colList.get(0)).asText(); }
 * 
 * colList = tableRow.getByXPath("//td[@align='left'][@valign='middle']"); if(
 * colList.size()>0){ productName = ((HtmlElement)colList.get(0)).asText();
 * }else{ logger.info("Product Name not available check query..."); }
 * 
 * 
 * //logger.info("Product = " + productName + " Size = " + size+ " Price = " +
 * price + " Product Id = " + productId);
 * 
 * ScrapeItemDTO wegmanItem = new ScrapeItemDTO (); if( currentDeptName != null)
 * wegmanItem.dept = currentDeptName; if( currentURL != null)
 * wegmanItem.shoppingURL = currentURL; wegmanItem.itemName = productName;
 * wegmanItem.itemUOMSize = size; wegmanItem.wegmansProductCode = productId;
 * wegmanItem.UPC = upc; if( price.indexOf('*') >=0 ){ //get the actual Prices
 * since the item is on Sale if( processSale) scrapeItembyProductCode(webClient,
 * wegmanItem); } else{ wegmanItem.regPrice = price; } wegmanItem.regPrice =
 * stripDollarSymbol(wegmanItem.regPrice); wegmanItem.salePrice =
 * stripDollarSymbol(wegmanItem.salePrice); splitSizeUOM(wegmanItem);
 * 
 * logger.info("Product = " + wegmanItem.itemName+ " Size = " +
 * wegmanItem.itemSize + " UOM = " + wegmanItem.itemUOMDesc + " Price = " +
 * wegmanItem.regPrice + " Sale Price = " + wegmanItem.salePrice + " UPC = " +
 * wegmanItem.UPC + " Product Id = " + wegmanItem.wegmansProductCode);
 * 
 * wegmanItemListArray.add(wegmanItem);
 * 
 * } }
 * 
 * }
 * 
 * private String replaceLineFeed(String productName) { productName =
 * productName.replaceAll("\n", " "); productName = productName.replaceAll("\r",
 * " "); return productName; }
 * 
 * 
 * private void splitSizeUOM(ScrapeItemDTO wegmanItem){
 * 
 * int index = wegmanItem.itemUOMSize.indexOf(' '); if (index >= 0){
 * wegmanItem.itemSize =wegmanItem.itemUOMSize.substring(0, index);
 * wegmanItem.itemUOMDesc =wegmanItem.itemUOMSize.substring(index+1); }
 * 
 * }
 * 
 * public void scrapeItembyProductCode(final WebClient webClient, ScrapeItemDTO
 * item) throws IOException, MalformedURLException { HtmlPage page; String
 * shoppingMenuURL; shoppingMenuURL =
 * "http://www.wegmans.com/webapp/wcs/stores/servlet/ProductDetailsMiniAJAXView?storeId=10052&catalogId=10002&langId=-1&productHasImage=false&productId="
 * +item.wegmansProductCode; //logger.info(shoppingMenuURL); page =
 * webClient.getPage(shoppingMenuURL); //logger.info(page.asXml());
 * parseProductPage(page, item); }
 * 
 * 
 * 
 * private void parseProductPage(HtmlPage page, ScrapeItemDTO itemObj) { String
 * response; response = page.asXml();
 * 
 * 
 * //logger.info(response); List<?> elmList =
 * page.getByXPath("//h2[@class='hoverHead']"); if(elmList.size() > 0) {
 * HtmlElement elm = (HtmlElement)elmList.get(0); //String text = elm
 * .getTextContent().trim(); //logger.info("item Name:" + text);
 * itemObj.wegmansItemName = elm .getTextContent().trim();
 * itemObj.wegmansItemName = replaceLineFeed(itemObj.wegmansItemName);
 * 
 * 
 * }
 * 
 * boolean onSale = false;
 * 
 * elmList = page.getByXPath("//span[@class='disclaimerPrice']");
 * if(elmList.size() > 0) { HtmlElement elm = (HtmlElement)elmList.get(0);
 * //String price = elm .getTextContent().trim();
 * //logger.info("item Regular price:" + price); itemObj.regPrice = elm
 * .getTextContent().trim(); onSale = true; }
 * 
 * elmList = page.getByXPath("//span[@class='mrkfltwd16']"); if(elmList.size() >
 * 0) { HtmlElement elm = (HtmlElement)elmList.get(0); String price =
 * elm.getTextContent().trim(); //logger.info("item price:" + price); if(
 * onSale) itemObj.salePrice = price; else itemObj.regPrice = price;
 * 
 * } elmList = page.getByXPath("//p[@class='sfCompareProductDesc']/b");
 * if(elmList.size() > 0) { HtmlElement elm = (HtmlElement)elmList.get(0);
 * //String size = elm .getTextContent().trim(); //logger.info("item size:" +
 * size); itemObj.itemUOMSize = elm.getTextContent().trim(); }
 * 
 * 
 * //Get the UPC and check if the valid UPC in that portion
 * 
 * 
 * elmList = page.getByXPath("//img[@class='prodImg200']"); if(elmList.size() >
 * 0) { HtmlElement elm = (HtmlElement)elmList.get(0); String xmlString =
 * elm.asXml(); String upc = this.getUPCFromXMLString(xmlString); //String
 * validUPCPart = itemObj.UPC.substring(2); if( upc.contains(itemObj.UPC))
 * itemObj.upcMatch = true; else{ itemObj.upcMatch = false;
 * //logger.info("Wegmans UPC = " + upc + "S&S UPC = " +itemObj.UPC); } if(
 * itemObj.wegmansItemName.contains("Wegmans") ||
 * itemObj.wegmansItemName.contains("TopCare")){ itemObj.upcMatch = true; }
 * 
 * } itemObj.regPrice = stripDollarSymbol(itemObj.regPrice); itemObj.salePrice =
 * stripDollarSymbol(itemObj.salePrice);
 * 
 * }
 * 
 * public boolean hasNextPageLink(HtmlPage page){
 * 
 * boolean hasNextPage = false;
 * 
 * List<?> elmList = page.getByXPath("//a[@alt='Next Page']"); if(elmList.size()
 * > 0) hasNextPage = true; return hasNextPage;
 * 
 * }
 * 
 * public void writeToCell( HSSFRow row, int colCount, String val) { if( val !=
 * null){ HSSFCell cell = row.createCell(colCount,HSSFCell.CELL_TYPE_STRING);
 * cell.setCellValue(val); } }
 * 
 * public String stripDollarSymbol(String price){
 * 
 * if( price != null && price.length()>0){ int dollarPosition =
 * price.indexOf('$'); if (dollarPosition == 0) return price.substring(1); }
 * return price; }
 * 
 * public double ConvertToDouble(String val){
 * 
 * double retVal = 0.0; try{ if( val.contains("/$")){ int multipleIndex =
 * val.indexOf("/$"); int quantity = Integer.parseInt(val.substring(0,
 * multipleIndex)); double price =
 * Double.parseDouble(val.substring(multipleIndex+2)); retVal = price/quantity;
 * 
 * }else{ retVal = Double.parseDouble(val); }
 * 
 * }catch(Exception e){ } return retVal; }
 * 
 * public double getPrice(String val){
 * 
 * double retVal = 0.0; try{ if( val.contains("/$")){ int multipleIndex =
 * val.indexOf("/$");
 * 
 * double price = Double.parseDouble(val.substring(multipleIndex+2)); retVal =
 * price;
 * 
 * }else{ retVal = Double.parseDouble(val); }
 * 
 * }catch(Exception e){ } return retVal; }
 * 
 * public int getQuantity(String val){
 * 
 * int retVal = 0; try{ if( val.contains("/$")){ int multipleIndex =
 * val.indexOf("/$"); int quantity = Integer.parseInt(val.substring(0,
 * multipleIndex)); retVal = quantity;
 * 
 * }else{ double price = Double.parseDouble(val); retVal = 1; }
 * 
 * }catch(Exception e){ } return retVal; }
 * 
 * public void splitUOM(String sizeUOM, ScrapeItemDTO item){ sizeUOM =
 * sizeUOM.toUpperCase(); if (sizeUOM!= null) { sizeUOM = sizeUOM.trim(); int i
 * = 0; while ( i < sizeUOM.length()){ int c = sizeUOM.charAt(i); // Non
 * character if (c < 65 || c > 90){ i++; continue; } else break; } item.itemSize
 * = sizeUOM.substring(0,i); item.itemUOMDesc = sizeUOM.substring(i).trim(); } }
 * 
 * public String normalizeUOM(String uom){ String newUOM = uom; if(
 * uom.equals("oz.")) newUOM = "OZ"; else if( uom.equals("ea.")) newUOM = "CT";
 * else if( uom.equals("ct.")) newUOM = "CT"; else if( uom.equals("fl. oz."))
 * newUOM = "FOZ"; else if( uom.equals("yd.")) newUOM = "YD"; else if(
 * uom.equals("gal.")) newUOM = "GAL"; else if( uom.equals("qt.")) newUOM =
 * "QT"; else if( uom.equals("pt.")) newUOM = "PT"; else if( uom.equals("lb."))
 * newUOM = "LB"; else if( uom.equals("lb.")) newUOM = "LB"; else if(
 * uom.equals("liter")) newUOM = "LT"; else if( uom.equals("ml.")) newUOM =
 * "MLT"; return newUOM;
 * 
 * }
 * 
 * public String standardizeUPC(String upc){ if( upc != null){ if( upc.charAt(0)
 * == '\'') upc = upc.substring(1); String prefix = ""; for ( int i = 0 ; i < 12
 * - upc.length(); i++) prefix += "0"; upc = "'" + prefix+ upc; }
 * 
 * return upc; }
 * 
 * public void cleanupWords(String [] words){
 * 
 * for ( int i = 0; i < words.length; i++){ words[i] = cleanupWord(words[i]); }
 * } public String cleanupWord(String word){
 * 
 * StringBuffer sb = new StringBuffer(); int wordLength = word.length(); for (
 * int i = 0; i < wordLength; i++){ char ch = word.charAt(i); if( ch != ',' &&
 * ch!= '\'' && ch!= '-' && ch!= '#') sb.append(ch); } return sb.toString(); }
 * 
 * public String getUPCFromXMLString(String upcXMLString){
 * 
 * String upc = ""; int upcEndIndex = upcXMLString.indexOf(".jpg");
 * 
 * int beginIndex = upcEndIndex - 13; for ( ; beginIndex >= 0; beginIndex-- ){
 * if( upcXMLString.charAt(beginIndex)=='/') break; }
 * 
 * beginIndex = beginIndex+1; if( upcEndIndex > 0 && beginIndex > 0) upc =
 * upcXMLString.substring(beginIndex , upcEndIndex);
 * 
 * return upc; }
 * 
 * }
 */
 }