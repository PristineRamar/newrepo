
package com.pristine.webscrape.shoprite;
 /* 
 * import org.apache.log4j.Logger; import org.apache.log4j.PropertyConfigurator;
 * import org.w3c.dom.NamedNodeMap; import org.w3c.dom.Node;
 * 
 * import java.net.*; import java.util.ArrayList; import java.util.HashMap;
 * import java.util.Iterator; import java.util.List; import
 * java.util.StringTokenizer; import java.io.*;
 * 
 * import com.gargoylesoftware.htmlunit.BrowserVersion; import
 * com.gargoylesoftware.htmlunit.WebClient; import
 * com.gargoylesoftware.htmlunit.html.DomNode; import
 * com.gargoylesoftware.htmlunit.html.HtmlAnchor; import
 * com.gargoylesoftware.htmlunit.html.HtmlDivision; import
 * com.gargoylesoftware.htmlunit.html.HtmlElement; import
 * com.gargoylesoftware.htmlunit.html.HtmlHeading1; import
 * com.gargoylesoftware.htmlunit.html.HtmlPage; import
 * com.gargoylesoftware.htmlunit.html.HtmlTable; import
 * com.gargoylesoftware.htmlunit.html.HtmlTableCell; import
 * com.gargoylesoftware.htmlunit.html.HtmlTableRow; import
 * com.pristine.util.PropertyManager; import com.pristine.webscrape.WebFile;
 * import java.util.StringTokenizer;
 */
 public class ShopriteScrape {
/*	 private static Logger logger =
 * Logger.getLogger("SRWebScraping");
 * 
 * private HashMap<String,String> categoryNameMap = new HashMap<String,String>
 * (); private HashMap<String,String> categoryHasChildMap = new
 * HashMap<String,String> ();
 * 
 * private String storeName; private String storeIdentifier;
 * 
 * private class ShopRiteProductInfo { public String upc; public String
 * productName; public String regPrice; public String salePrice; public String
 * brand; public String size; public String uom; public ProductHierarchy
 * productCategory;
 * 
 * } private class ProductHierarchy{ public String catL0; public String catL1;
 * public String catL2; public String catL3; public String catL0FullName; public
 * String catL1FullName; public String catL2FullName; public String
 * catL3FullName; }
 * 
 * public static void main(String[] args) {
 * PropertyConfigurator.configure("log4j.properties");
 * PropertyManager.initialize("analysis.properties");
 * 
 * logger.info("Shoprite Web scraping started ....."); try { ShopriteScrape ss =
 * new ShopriteScrape(); //"strid=3C55827";"strid=3572346";
 * ss.performScraping("3572346"); }catch(Exception e){
 * logger.error("Exception occured",e); }
 * 
 * logger.info("Shoprite Web scraping done .....");
 * 
 * }
 * 
 * public void performScraping(String strId) throws Exception {
 * //readFromURL("http://shop.mywebgrocer.com/shop.aspx?strid=3572346"); final
 * WebClient webClient = new WebClient(BrowserVersion.INTERNET_EXPLORER_8);
 * webClient.setCssEnabled(false); webClient.setJavaScriptEnabled(false); strId
 * = "strid=" +strId; HtmlPage page = webClient.getPage(
 * "http://shop.mywebgrocer.com/shop.aspx?&sid=37096191&sid_guid=ea5dcfb3-1a4e-4c7b-93a7-255c35e9d5fe&"
 * + strId);
 * 
 * String response = page.toString(); logger.info(response); String sid ="";
 * String sid_guid = ""; StringTokenizer st = new StringTokenizer(response,
 * "&");
 * 
 * //parse the session id to create a new URL int i = 0; while
 * (st.hasMoreElements()){ String tempStr = st.nextToken(); if( i == 1) sid =
 * tempStr; else if ( i== 2) sid_guid = tempStr; i++; }
 * 
 * String shoppingMenuURL =
 * "http://shop.mywebgrocer.com/ShoppingMenu.aspx?&"+sid+"&" +sid_guid + "&" +
 * strId; //logger.info(shoppingMenuURL);
 * 
 * page = webClient.getPage(shoppingMenuURL); List<?> titleList =
 * page.getByXPath("/html/head/title");
 * 
 * if( titleList.size() > 0) { storeName =
 * ((HtmlElement)titleList.get(0)).asText(); logger.info( "Store Name is " +
 * storeName); }
 * 
 * HtmlDivision div = page.getHtmlElementById("ShoppingMenuBorder"); List<?>
 * hList = div.getByXPath("//h4"); setupHashMap(hList); hList =
 * div.getByXPath("//h3"); setupHashMap(hList); hList = div.getByXPath("//h2");
 * setupHashMap(hList); hList = div.getByXPath("//h1"); setupHashMap(hList);
 * 
 * Iterator<?> itr = hList.iterator(); while(itr.hasNext()){ HtmlElement h1 =
 * (HtmlElement)itr.next(); logger.info("Id1 = " + h1.getAttribute("id") +
 * "Id 2 =" + h1.getId() + "Value = " + h1.getTextContent() ); }
 * 
 * 
 * //Looping logic ArrayList<ShopRiteProductInfo> itemList = new
 * ArrayList<ShopRiteProductInfo> ();
 * 
 * Iterator <String> categoryItr = categoryHasChildMap.keySet().iterator(); int
 * count = 0; while (categoryItr.hasNext()) { String categoryKey =
 * categoryItr.next(); String hasProducts =
 * categoryHasChildMap.get(categoryKey); if( hasProducts.equals("true")){
 * logger.info("analyzing category " + categoryKey); ProductHierarchy
 * productCategory = this.buildProductHierarchy(categoryKey); ArrayList<String>
 * prdURLList = getProductList(webClient, strId, sid, sid_guid,
 * productCategory);
 * 
 * String productURL =
 * "http://shop.mywebgrocer.com/ProductDetail.aspx?&"+sid+"&" +sid_guid + "&" +
 * strId +"&pid=686668";
 * 
 * logger.info( "Level 0 = " + productCategory.catL0FullName + ",Level 1 = " +
 * productCategory.catL1FullName + "Level 2 = " + productCategory.catL2FullName
 * + "Level 3 = " + productCategory.catL3FullName);
 * 
 * logger.info("Product count = " + prdURLList.size()); Iterator<String>
 * prdURLItr =prdURLList.iterator(); while(prdURLItr.hasNext()){ productURL =
 * prdURLItr.next(); ShopRiteProductInfo pInfo = getProductInfo(webClient,
 * productURL); pInfo.productCategory = productCategory; itemList.add(pInfo);
 * logger.info( pInfo.brand + " " + pInfo.productName + " " + pInfo.size + " " +
 * pInfo.uom + " " + pInfo.upc + " " + pInfo.regPrice + " (" + pInfo.salePrice +
 * ")") ; } } count++; //if( count == 5)break; }
 * 
 * logger.info ( "Total Item Count = " + itemList.size());
 * 
 * 
 * 
 * }
 * 
 * private ArrayList<String> getProductList(final WebClient webClient, String
 * strId, String sid, String sid_guid, ProductHierarchy productCategory) throws
 * IOException, MalformedURLException { HtmlPage page;
 * 
 * String browsingURL = "http://shop.mywebgrocer.com/Browse.aspx?&"+sid+"&"
 * +sid_guid + "&" + strId
 * +"&catL0=570&catL1=587&catL2=725&catL3=1566&HasProducts=1&sti=0&currentposition=0";
 * 
 * String browsingURL = "http://shop.mywebgrocer.com/Browse.aspx?&"+sid+"&"
 * +sid_guid + "&" + strId +"&catL0="+productCategory.catL0 +
 * "&catL1="+productCategory.catL1 +"&catL2="+ productCategory.catL2
 * +"&catL3="+productCategory.catL3+"&HasProducts=1&sti=0&currentposition=0";
 * logger.debug( browsingURL); boolean nextPageURL = true; ArrayList <String>
 * prdURLList = new ArrayList <String> (); while (nextPageURL) { nextPageURL =
 * false; page = webClient.getPage(browsingURL); List<?> anchorList =
 * page.getAnchors(); Iterator<?> itr = anchorList.iterator();
 * while(itr.hasNext()){ HtmlAnchor anc = (HtmlAnchor)itr.next(); String url =
 * anc.getHrefAttribute();
 * 
 * if ( url.contains("ProductDetail.aspx")){ prdURLList.add(url);
 * 
 * }else if (anc.getId().equals("btnNext_bottom")){
 * //logger.info("******* Next Page URL href = " + url ); nextPageURL = true;
 * browsingURL = url; } } } return prdURLList; }
 * 
 * private void setupHashMap( List<?> hList) {
 * 
 * 
 * Iterator<?> itr = hList.iterator(); while(itr.hasNext()){ HtmlElement header
 * = (HtmlElement)itr.next(); String id = header.getId(); String value =
 * header.getTextContent(); categoryNameMap.put(id, value); if(
 * categoryHasChildMap.containsKey(id)) continue; else {
 * categoryHasChildMap.put(id, "true"); while ( id != null ) { id =
 * getCategoryParent(id); if( value != null) categoryHasChildMap.put(id,
 * "false");
 * 
 * } } } }
 * 
 * 
 * private ShopRiteProductInfo getProductInfo(final WebClient webClient, String
 * productURL) throws Exception {
 * 
 * HtmlPage page = webClient.getPage(productURL); if( page == null) return null;
 * HtmlTable table = page.getHtmlElementById("tblProductDetail");
 * //logger.info(table.asXml()); ShopRiteProductInfo pInfo = new
 * ShopRiteProductInfo ();
 * 
 * List<?> elmList = table.getByXPath("//b[@class='ProductDetail-Brand']");
 * if(elmList.size() > 0) { HtmlElement elm = (HtmlElement)elmList.get(0);
 * pInfo.brand = elm .getTextContent().trim(); //logger.info("Brand - " + elm
 * .getTextContent().trim()); }
 * 
 * elmList = table.getByXPath("//b[@class='ProductDetail-ProductName']");
 * if(elmList.size() > 0) { HtmlElement elm = (HtmlElement)elmList.get(0);
 * pInfo.productName = elm .getTextContent().trim(); //logger.info("Item - " +
 * (elm .getTextContent().trim())); } elmList =
 * table.getByXPath("//b[@class='sm']"); if(elmList.size() > 0) { HtmlElement
 * elm = (HtmlElement)elmList.get(0); String text = elm
 * .getTextContent().trim(); //logger.info("size/uom - " + text); int index =
 * getNonCharIndex(text); pInfo.size = text.substring(0, index).trim();
 * pInfo.uom = text.substring(index).trim(); } elmList =
 * table.getByXPath("tbody/tr/td/table/tbody/tr/td/div"); if(elmList.size() > 0)
 * { HtmlElement elm = (HtmlElement)elmList.get(0); DomNode dm =
 * elm.getParentNode(); String skuArea = dm.asText(); int index =
 * skuArea.indexOf("SKU/UPC:"); if( index >=0){ pInfo.upc =
 * skuArea.substring(index+8).trim(); if( pInfo.upc.length() > 12) pInfo.upc =
 * pInfo.upc.substring(0,12); } }
 * 
 * //Get the Price elmList =
 * table.getByXPath("//td[@class='ProductUnitDetailTDLeft']"); if(elmList.size()
 * > 0) { HtmlElement elm = (HtmlElement)elmList.get(0); pInfo.regPrice =
 * elm.asText().trim(); int index = pInfo.regPrice.indexOf("Price"); if (index
 * >0) pInfo.regPrice = pInfo.regPrice.substring(0, index).trim(); } // Check if
 * Sale area is present elmList = table.getByXPath("//tr[@bgcolor='#fffff']");
 * if(elmList.size() > 0) { HtmlElement elm = (HtmlElement)elmList.get(0);
 * String regPrice = elm.asText(); int index = regPrice.indexOf("Reg. Price:");
 * regPrice = regPrice.substring(index+11).trim();
 * 
 * //Note - the previous price captured is Sale Price, now move it Sale price
 * field pInfo.salePrice = pInfo.regPrice; pInfo.regPrice = regPrice; } return
 * pInfo; }
 * 
 * private ProductHierarchy buildProductHierarchy(String value){
 * ProductHierarchy productCategory = new ProductHierarchy (); String[]
 * prodHierarchy = splitProductHierachy(value); productCategory.catL0 =
 * prodHierarchy[0]; productCategory.catL1 = prodHierarchy[1];
 * productCategory.catL2 = prodHierarchy[2]; productCategory.catL3 =
 * prodHierarchy[3];
 * 
 * if(productCategory.catL3.equals("-1")) productCategory.catL3FullName = null;
 * else { productCategory.catL3FullName = categoryNameMap.get(value); value =
 * getCategoryParent(value); }
 * 
 * if(productCategory.catL2.equals("-1")) productCategory.catL2FullName = null;
 * else {
 * 
 * productCategory.catL2FullName = categoryNameMap.get(value); value =
 * getCategoryParent(value); }
 * 
 * if(productCategory.catL1.equals("-1")) productCategory.catL1FullName = null;
 * else { productCategory.catL1FullName = categoryNameMap.get(value); value =
 * getCategoryParent(value); } productCategory.catL0FullName =
 * categoryNameMap.get(value);
 * 
 * return productCategory;
 * 
 * }
 * 
 * private String getCategoryParent(String value){ String[] prodHierarchy =
 * splitProductHierachy(value); boolean goUpHierarchy = true; if( goUpHierarchy
 * && !prodHierarchy[3].equals("-1")) { goUpHierarchy = false; prodHierarchy[3]
 * = "-1"; } if( goUpHierarchy && !prodHierarchy[2].equals("-1")) {
 * goUpHierarchy = false; prodHierarchy[2] = "-1"; } if( goUpHierarchy &&
 * !prodHierarchy[1].equals("-1")) { goUpHierarchy = false; prodHierarchy[1] =
 * "-1"; } String retVal; if( goUpHierarchy ) retVal = null; else retVal =
 * prodHierarchy[0] + "_" + prodHierarchy[1] + "_" + prodHierarchy[2] + "_" +
 * prodHierarchy[3]; //logger.info("Parent of " + value + " is " + retVal);
 * return retVal; }
 * 
 * String[] splitProductHierachy(String value) { //logger.info(" input " +
 * value); int i = 0; String [] splitHierarchy = new String[4]; StringTokenizer
 * st = new StringTokenizer(value, "_"); while (st.hasMoreTokens()) {
 * splitHierarchy[i]=st.nextToken(); i++; } return splitHierarchy;
 * 
 * }
 * 
 * // Used for Size/UOM splitting private int getNonCharIndex(String text) { int
 * index = -1; int i = 0; char [] textArr = text.toCharArray();
 * 
 * while ( i < textArr.length){ if ( !Character.isDigit(textArr[i]) &&
 * textArr[i] != '.') break; i++; } if ( i < textArr.length) index = i; return
 * index; }
 * 
 * Product detail XML used for Parsing
 * 
 * <table class="ProductDetail" id="tblProductDetail" cellspacing="0"
 * cellpadding="0" border="0"> <tbody> <tr> <td bgcolor="#ffffff" height="196">
 * <!-- TABLE PRODUCT PRICE INFO --> <table border="0" width="100%"
 * height="100%"> <tbody> <tr> <td width="50%" valign="middle"> <b
 * class="ProductDetail-Brand">
 * 
 * Beneful
 * 
 * </b> <div> <b class="ProductDetail-ProductName">
 * 
 * Dog Food - Playful Life </b> </div> <br/> <b style="font-size: 11px;"> <br/>
 * <b class="sm"> 15.00 lb </b> <br/> <br/>
 * 
 * SKU/UPC: 017800126311
 * 
 * 
 * <div> <br/> <b class="tagline"/> </div> </b> </td> <td> <div
 * class="ImageTag"> <img style="border:0px;"
 * src="http://shop.mywebgrocer.com/i/Product/DJ/8/686668.jpg"/> </div> </td>
 * </tr> <tr> <td colspan="2" align="center" valign="middle"> <!-- TABLE SALE
 * DETAIL --> <table border="0" cellpadding="2" width="100%"> <tbody> <tr
 * bgcolor="#fffff"> <td> <img src="c/s/3C55827/RegSaleFlag0.gif" width="16"
 * height="15"/> </td> <td> <b class="SaleLine" style="font-weight:bold;"> On
 * Sale! Reduced Price! <br/> (Limit: 4 Items) </b> </td> <td> Reg. Price:
 * $15.49 </td> </tr> </tbody> </table> <!-- END TABLE SALE DETAIL --> </td>
 * </tr> </tbody> </table> <!-- END TABLE PRODUCT PRICE DETAIL --> </td> </tr>
 * <tr> <td class="ProductUnitDetail"> <!-- TABLE PRODUCT UNIT DETAIL --> <table
 * border="0" cellspacing="0" cellpadding="0" width="100%" height="100%"
 * valign="top"> <tbody> <tr> <td class="ProductUnitDetailTD"> <b
 * style="color:#e23621;"> $0.97/lb </b> <div> <b
 * style="font-size:10px;color:#e23621;"> Per Unit </b> </div> </td> <td
 * class="ProductUnitDetailTDLeft"> <b class="Bold" style="color: #e23621;">
 * 
 * $14.49
 * 
 * </b> <div style="font-size: 10px; color: #e23621;">
 * 
 * Price </div> </td> <td class="ProductUnitDetailTD"> <b
 * id="AttributeIDSection"/> </td> </tr> <tr> <td colspan="3"
 * class="ProductOperations"> <div> <a id="btnProdDetailBuy"
 * name="btnProdDetailBuy" class="btn1b actionButton Bold"
 * href="javascript:Buy(&apos;frmProductDetail_Buy&apos;,686668,1,-1,false);"
 * title="Add Beneful Dog Food - Playful Life 15.00 lb to your Shopping Cart."
 * onmouseout="this.className=&apos;btn1b actionButton Bold&apos;"
 * onmouseover="this.className=&apos;btn1bhov actionButton Bold&apos;"> Add to
 * Cart </a> <b class="btnSpc"/> <a id="btnProductDetailSave"
 * name="btnProductDetailSave" class="listLink"
 * href="javascript:alert(&apos;Please sign in before adding items to your active list.&apos;);"
 * title="Save to (active) List"> Save to List </a> </div> </td> </tr> </tbody>
 * </table> <!-- END TABLE PRODUCT UNIT DETAIL --> </td> </tr> </tbody> </table>
 * 
 * Menu Structure <div id='menuTitle' class='roundedContainer'>Shop
 * Departments</div><ul id='ShoppingMenuClient00_ShoppingMenu'
 * class='ShoppingMenu roundedContainer'> <li><h1
 * id='570_-1_-1_-1'>Grocery</h1><ul><li> <h2 id='570_572_-1_-1'>Baby
 * Store</h2><ul><li> <h3 id='570_572_616_-1'>Baby Bath & Skin
 * Care</h3></li><li> <h3 id='570_572_612_-1'>Baby Foods</h3><ul><li> <h4
 * id='570_572_612_1124'>1st Foods</h4></li><li><h4 id='570_572_612_1496'>2nd
 * Foods</h4></li><li> <h4 id='570_572_612_1497'>3rd Foods</h4></li><li><h4
 * id='570_572_612_829'>Cereals & Snacks</h4></li> <li><h4
 * id='570_572_612_830'>Juices & Water</h4></li><li><h4
 * id='570_572_612_1128'>Organic</h4></li><li> <h4
 * id='570_572_612_1123'>Toddlers</h4></li></ul></li><li><h3
 * id='570_572_614_-1'>Baby Medications</h3></li><li><h3
 * id='570_572_610_-1'>Bottles & Nursing </h3></li><li><h3
 * id='570_572_613_-1'>Diapering Needs </h3><ul><li><h4
 * id='570_572_613_611'>Diapers</h4></li><li><h4 id='570_572_613_1495'>Training
 * Pants</h4> </li><li><h4 id='570_572_613_1484'>Wipes & Creams</
 */
 }
 