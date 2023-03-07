package com.pristine.dataload.riteaid;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dao.riteaid.PromoDAO;
import com.pristine.dataload.tops.PriceDataLoad;
import com.pristine.dto.PromoDataStandardDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.riteaid.RAPromoDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRFormatHelper;

public class PromoCorrector {

	private static Logger logger = Logger.getLogger("PromoCorrector");
	private final static String END_DATE = "END_DATE=";
	private final static String START_DATE = "START_DATE=";
	private static Connection conn = null;
	List<RAPromoDTO> eventfile = null;
	DateFormat appDateFormatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	SimpleDateFormat sdf = new SimpleDateFormat(Constants.APP_DATE_FORMAT);

	List<PromoDataStandardDTO> tempList = null;
	PriceDataLoad objPriceLoad = new PriceDataLoad();
	HashMap<String, HashMap<Integer, List<PromoDataStandardDTO>>> promoMap = null;
	Set<String> itemCodeNotFound = null;
	static String startDate, endDate = "";
	HashMap<Integer, Integer> zoneAndItsStoreCount = new HashMap<>();
	StoreDAO storeDAO = new StoreDAO();
	HashMap<String, Integer> calendarMap = new HashMap<String, Integer>();
	RetailCalendarDAO calDAO = new RetailCalendarDAO();
	Set<Long> promodefIdTodelete = new HashSet<Long>();
	PromoDAO promoDAO = new PromoDAO();
	List<PromoDataStandardDTO> updateList = new ArrayList<>();

	int totalzones = 12;

	public PromoCorrector() {

		PropertyManager.initialize("analysis.properties");

		try {
			conn = DBManager.getConnection();

		} catch (GeneralException exe) {
			logger.error("Error while connecting DB:" + exe);
			System.exit(1);
		}

	}

	public static void main(String[] args) throws GeneralException {

		PropertyConfigurator.configure("log4j.properties");
		PropertyManager.initialize("analysis.properties");
		for (String arg : args) {
			if (arg.startsWith(END_DATE)) {
				endDate = arg.substring(END_DATE.length());
			}

			if (arg.startsWith(START_DATE)) {
				startDate = arg.substring(START_DATE.length());
			}
		}

		try {
			logger.info("promo corrector Started for:- " + startDate + " End Date: " + endDate);
			PromoCorrector promoCorrector = new PromoCorrector();
			promoCorrector.initialize();
			HashMap<String, Integer> dateMap = promoCorrector.splitPromoByWeek(startDate, endDate);

			promoCorrector.getPromos(dateMap);
			logger.info(
					"*********promo corrector Complete for:- " + startDate + " End Date: " + endDate + " *********");
		} catch (Exception e) {

			logger.error("Error occured in promoCorrector() main method", e);
		}

	}

	public void initialize()

	{
		zoneAndItsStoreCount = storeDAO.getPriceZoneStrcount(conn);
		logger.info("storeZone count:" + zoneAndItsStoreCount.size());

	}

	public HashMap<String, Integer> splitPromoByWeek(String startDate, String endDate)
			throws ParseException, GeneralException {
		HashMap<String, Integer> dateList = new HashMap<String, Integer>();

		Date promoStartDate = getFirstDateOfWeek(sdf.parse(startDate));
		Date promoEndDate = getLastDateOfWeek(sdf.parse(endDate));

		Long diff = promoEndDate.getTime() - promoStartDate.getTime();
		long diffDates = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);

		if (diffDates > 7) {

			float dif = (float) diffDates / 7;
			int noOfWeeks = (int) Math.ceil(dif);

			for (int i = 0; i < noOfWeeks; i++) {

				String start = sdf.format(promoStartDate);
				promoEndDate = DateUtil.incrementDate(promoStartDate, 6);
				promoStartDate = DateUtil.incrementDate(promoStartDate, 7);

				int calId = getCalendarId(start);

				logger.debug("Weekstartdate:" + start + "end date: " + sdf.format(promoEndDate));
				dateList.put(start + "-" + promoEndDate, calId);
			}
		} else {
			int calId = getCalendarId(sdf.format(promoStartDate));

			dateList.put(sdf.format(promoStartDate) + "-" + sdf.format(promoEndDate), calId);
		}
		return dateList;

	}

	private int getCalendarId(String inputDate) throws GeneralException {
		if (calendarMap.get(inputDate) == null) {
			int startCalId = ((RetailCalendarDTO) calDAO.getCalendarId(conn, inputDate, Constants.CALENDAR_WEEK))
					.getCalendarId();
			calendarMap.put(inputDate, startCalId);
		}

		return calendarMap.get(inputDate);
	}

	int max = 0;
	int maxzone = 0;

	public void getPromos(HashMap<String, Integer> dateMap) {

		dateMap.forEach((startDate, calId) -> {

			promoMap = new HashMap<String, HashMap<Integer, List<PromoDataStandardDTO>>>();
			try {
				String startWeekDate = startDate.split("-")[0];
				logger.info("getPromos()- Getting promos for week:  " + startWeekDate);
				promoMap = promoDAO.getWeeklyPromotions(conn, startWeekDate);
				logger.debug("getPromos()- Data fetched: - " + promoMap.size());

				for (Map.Entry<String, HashMap<Integer, List<PromoDataStandardDTO>>> entry : promoMap.entrySet()) {

					max = 0;
					maxzone = 0;
					HashMap<Integer, List<PromoDataStandardDTO>> zoneList = entry.getValue();

					int totalPZones = zoneList.size();
					logger.info(
							"getPromos()- Checking for event : - " + entry.getKey() + "Total zones : " + totalPZones);

					if (totalPZones >= totalzones) {

						HashMap<Integer, List<PromoDataStandardDTO>> zoneListWithAd = new HashMap<>();
						
						zoneList.forEach((zoneNo, events) -> {
							List<PromoDataStandardDTO> itemsWithPageBlock = events.stream()
									.filter(p -> p.getPageNumber() != null).collect(Collectors.toList());
							if(itemsWithPageBlock.size() > 0) {
								zoneListWithAd.put(zoneNo, events);
							}
						});

							
						if(zoneListWithAd.size() > 0) {
							zoneList = zoneListWithAd;
							logger.info("getPromos() - Ad event: " + entry.getKey());
						}
						
						zoneList.forEach((zoneNo, events) -> {

							if (zoneAndItsStoreCount.containsKey(zoneNo)) {
								int stores = zoneAndItsStoreCount.get(zoneNo);

								if (max < stores) {
									max = stores;
									maxzone = zoneNo;
								}
							}

						});

						if (entry.getValue().containsKey(maxzone)) {
							updateList = entry.getValue().get(maxzone);
							Set<String> distinctItems = updateList.stream().map(PromoDataStandardDTO::getItemCode)
									.collect(Collectors.toSet());
							List<String> itemCodeList = new ArrayList<>(distinctItems);
							HashMap<String, RetailPriceDTO> currentWeekPrice = objPriceLoad.getRetailPrice(conn, " 52",
									itemCodeList, calId, null, Constants.ZONE_LEVEL_TYPE_ID);

							for (String prestoItmCode : itemCodeList) {

								updateList.forEach(item -> {

									if (item.getItemCode().equals(prestoItmCode)) {
										if (currentWeekPrice.containsKey(prestoItmCode)) {
											RetailPriceDTO retailPriceDTO = currentWeekPrice.get(prestoItmCode);
											setPrice(item, retailPriceDTO, prestoItmCode);
											setSalePrice(item);
										}
									}

								});
							}

						}

						zoneList.forEach((zoneNo, events) -> {

							if (zoneNo != maxzone) {

								Set<Long> distinctPromoDef1 = events.stream().map(PromoDataStandardDTO::getPromoDefId)
										.collect(Collectors.toSet());

								for (long id : distinctPromoDef1) {
									promodefIdTodelete.add(id);

								}
							}

						});

					} 

					try {
						promoDAO.deletePromotions(promodefIdTodelete, conn);
						PristineDBUtil.commitTransaction(conn, "Delete Promotions");
						promodefIdTodelete.clear();
					} catch (Exception ex) {
						PristineDBUtil.rollbackTransaction(conn, "Delete Promotions Rollback");
						logger.info("exception in deleting" + ex);

					}

					try {
						if (updateList.size() > 0) {
							logger.debug("Updating promotions started for #- " + updateList.size());
							promoDAO.updatePromotions(updateList, conn);
							updateList.clear();
							PristineDBUtil.commitTransaction(conn, "Update Promotions");
							// logger.info("Updating promotions Complete");
						} else {
							logger.debug("No update");
						}
					} catch (Exception ex) {
						PristineDBUtil.rollbackTransaction(conn, "Update Promotions Rollback");
						logger.info("exception in Update" + ex);
					}

				}

			} catch (GeneralException e) {

				logger.error("getPromos() Exception -" + e);
			}

		});

	}

	private void setSalePrice(PromoDataStandardDTO item) {

		double regPrice = Double.parseDouble(item.getEverydayPrice());

		if (item.getPromoTypeId() == Constants.PROMO_TYPE_STANDARD) {
			if (item.getPromoSubtypeId() == Constants.PROMO_SUB_TYPE_STANDARD_DOLLAR_OFF) {

				double salePrice = regPrice - item.getOfferValue();
				item.setSalePrice(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(salePrice)));

			} else if (item.getPromoSubtypeId() == Constants.PROMO_SUB_TYPE_STANDARD_PCT_OFF) {

				double discount = (item.getOfferValue() * regPrice) / 100;
				double salePrice = regPrice - discount;
				item.setSalePrice(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit((salePrice))));
			}

		} else if (item.getPromoTypeId() == Constants.PROMO_TYPE_BOGO) {
			if (item.getPromoSubtypeId() == Constants.PROMO_SUB_TYPE_BOGO_X_PCT_OFF) {
				double discount = (item.getOfferValue() * regPrice) / 100;
				double OfferItemPr = regPrice - discount;
				double salePrice = regPrice + OfferItemPr;
				item.setSalePrice(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(salePrice)));
				item.setSaleQty(2);
			} else if (item.getPromoSubtypeId() == Constants.PROMO_SUB_TYPE_BOGO_X_DOLLAR_OFF) {
				double OfferItemPr = regPrice - item.getOfferValue();
				double salePrice = regPrice + OfferItemPr;
				item.setSalePrice(salePrice);
				item.setSaleQty(2);
			} else {
				item.setSalePrice(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(regPrice)));
				item.setSaleQty(2);
			}

		} else if (item.getPromoTypeId() == Constants.PROMO_TYPE_BXGY_SAME) {
			if (item.getPromoSubtypeId() == Constants.PROMO_SUB_TYPE_BXGY_SAME_X_PCT_OFF) {
				double discount = (item.getOfferValue() * regPrice) / 100;
				double offerPrice = regPrice - discount;
				double salePrice = regPrice + offerPrice;
				item.setSalePrice(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(salePrice)));

			} else if (item.getPromoSubtypeId() == Constants.PROMO_SUB_TYPE_BXGY_SAME_X_DOLLAR_OFF) {
				double offerprice = regPrice - item.getOfferValue();
				double offerPrice = regPrice + offerprice;
				double salePrice = regPrice + offerPrice;
				item.setSalePrice(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(salePrice)));
				item.setSaleQty(2);
			} else {
				double salePrice = regPrice * item.getBuyQty();
				item.setSalePrice(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(salePrice)));
			}

		} else if (item.getPromoTypeId() == Constants.PROMO_TYPE_BXGY_DIFF) {
			if (item.getOffer_unit_type().equalsIgnoreCase("P")) {
				double discount = (item.getOfferValue() * regPrice) / 100;
				double offerPr = regPrice - discount;
				Double salePrice = Double
						.valueOf(PRFormatHelper.roundToTwoDecimalDigit((offerPr + regPrice) * item.getBuyQty()));
				item.setSalePrice(salePrice);
			} else if (item.getOffer_unit_type().equalsIgnoreCase("D")) {
				double offerPr = regPrice - item.getOfferValue();
				Double salePrice = Double
						.valueOf(PRFormatHelper.roundToTwoDecimalDigit((offerPr + regPrice) * item.getBuyQty()));
				item.setSalePrice(salePrice);
			} else {
				double salePrice = regPrice * item.getBuyQty();
				item.setSalePrice(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(salePrice)));
			}

		} else if (item.getPromoTypeId() == Constants.PROMO_TYPE_IN_AD_COUPON) {
			double offerPr = regPrice - item.getOfferValue();
			Double salePrice = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(offerPr));
			item.setSalePrice(salePrice);

		}

	}

	private void setPrice(PromoDataStandardDTO promoStandardDTO, RetailPriceDTO retailPriceDTO, String prestoItmCode) {

		String regprice = retailPriceDTO.getRegMPrice() > 0 ? String.valueOf(retailPriceDTO.getRegMPrice())
				: String.valueOf(retailPriceDTO.getRegPrice());
		String regQnty = retailPriceDTO.getRegQty() == 0 ? "1" : String.valueOf(retailPriceDTO.getRegQty());

		promoStandardDTO.setEverydayPrice(regprice);
		promoStandardDTO.setEverdayQty(regQnty);
	}

	private Date getFirstDateOfWeek(Date inputDate) throws ParseException {
		String strDate = DateUtil.getWeekStartDate(inputDate, 0);
		Date outputDate = appDateFormatter.parse(strDate);
		return outputDate;
	}

	/**
	 * 
	 * @param inputDate
	 * @return week end date for a given date
	 * @throws ParseException
	 */
	private Date getLastDateOfWeek(Date inputDate) throws ParseException {
		String strDate = DateUtil.getWeekStartDate(inputDate, 0);
		Date endDate = appDateFormatter.parse(strDate);
		Date outputDate = DateUtil.incrementDate(endDate, 6);
		return outputDate;
	}

}
