package com.pristine.service.offermgmt.substitute;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RList;
import org.rosuda.JRI.Rengine;

import com.pristine.dto.offermgmt.substitute.SubstituteAdjRInputDTO;
import com.pristine.dto.offermgmt.substitute.SubstituteAdjROutputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class SubstituteAdjustmentRServiceImpl implements SubstituteAdjustmentRService {
	private static Logger logger = Logger.getLogger("SubstituteAdjustmentRService");
	boolean isOnline = false;
	
	public SubstituteAdjustmentRServiceImpl(boolean isOnline) {
		this.isOnline = isOnline;
	}

	public List<SubstituteAdjROutputDTO> getSubsAdjustedMov(SubstituteAdjRInputDTO substituteInput) throws GeneralException {
		// When more than one user calls the prediction from UI, do sequentially
		List<SubstituteAdjROutputDTO> substituteAdjROutputDTO = new ArrayList<SubstituteAdjROutputDTO>();
		synchronized (SubstituteAdjustmentRServiceImpl.class) {
			logger.info("getSubsAdjustedMov() start");
			try {
				Rengine re = getREngine();
				try {
					substituteAdjROutputDTO = executeRProgram(re, substituteInput);
				} catch (Exception e) {
					logger.error("Error in getSubsAdjustedMov(): " + e.getMessage(), e);
					throw new GeneralException(e.toString());
				} finally {
					// Keep the thread if it is called from the batch program
					if (!isOnline) {
						re.end();
						logger.debug("R instance closed");
					}
				}
			} catch (Exception e) {
				logger.error("Error while creation Rengine in getSubsAdjustedMov(): " + e.getMessage(), e);
				throw new GeneralException(e.toString());
			}
		}
		return substituteAdjROutputDTO;
	}

	private Rengine getREngine() throws GeneralException {
		logger.info("getREngine() start");
		if (!Rengine.versionCheck()) {
			logger.error("** Version mismatch - Java files don't match library version.");
			throw new GeneralException("** Version mismatch - Java files don't match library version.");
		}

		logger.info("Creating Rengine (with arguments)");

		Rengine engine = Rengine.getMainEngine();
		if (engine == null) {
			logger.debug("Creating R Instance");
			engine = new Rengine(new String[] { "--vanilla" }, false, null);
		} else {
			logger.debug("R Instance already Exists");
		}
		logger.info("getREngine() end");
		return engine;
	}

	private List<SubstituteAdjROutputDTO> executeRProgram(Rengine re, SubstituteAdjRInputDTO substituteInput) throws GeneralException {
		List<SubstituteAdjROutputDTO> substituteAdjROutputDTO = new ArrayList<SubstituteAdjROutputDTO>();

		try {
			String subsAdjRRootPath = PropertyManager.getProperty("SUBS_ADJ_R_ROOT_PATH");
			String subsAdjRSrcPath = PropertyManager.getProperty("SUBS_ADJ_R_SRC_PATH");
			String logPath = PropertyManager.getProperty("SUBS_ADJ_LOG_PATH");
			logInputValues(substituteInput);
			
			//22nd Dec 2016, if there is no active substitute item, then don't call it
			if (substituteInput.hasAtlteastOneSubsItem()) {

				re.eval("locationLevelId <- " + substituteInput.getLocationLevelId() + ";", false);
				re.eval("locationId <- " + substituteInput.getLocationId() + ";", false);
				re.eval("productLevelId <- " + substituteInput.getProductLevelId() + ";", false);
				re.eval("productId <- " + substituteInput.getProductId() + ";", false);

				re.assign("mainItemCodes", substituteInput.getIntArray("MAIN_ITEM_CODE"));
				re.assign("mainItemRecWeekRegMultiple", substituteInput.getIntArray("MAIN_ITEM_REC_WEEK_REG_MULTIPLE"));
				re.assign("mainItemRecWeekRegPrice", substituteInput.getDoubleArray("MAIN_ITEM_REC_WEEK_REG_PRICE"));
				re.assign("mainItemRecWeekSaleMultiple", substituteInput.getIntArray("MAIN_ITEM_REC_WEEK_SALE_MULTIPLE"));
				re.assign("mainItemRecWeekSalePrice", substituteInput.getDoubleArray("MAIN_ITEM_REC_WEEK_SALE_PRICE"));
				re.assign("mainItemRegPred", substituteInput.getDoubleArray("MAIN_ITEM_REG_PRED"));
				re.assign("mainItemSalePred", substituteInput.getDoubleArray("MAIN_ITEM_SALE_PRED"));
				re.assign("subsItemCodes", substituteInput.getSubStringArray("SUB_ITEM_CODE"));
				re.assign("subsLevelId", substituteInput.getSubStringArray("SUB_LEVEL_ID"));
				re.assign("subsRecWeekRegMultiple", substituteInput.getSubStringArray("SUB_REC_WEEK_REG_MULTIPLE"));
				re.assign("subsRecWeekRegPrice", substituteInput.getSubStringArray("SUB_REC_WEEK_REG_PRICE"));
				re.assign("subsRecWeekSaleMultiple", substituteInput.getSubStringArray("SUB_REC_WEEK_SALE_MULTIPLE"));
				re.assign("subsRecWeekSalePrice", substituteInput.getSubStringArray("SUB_REC_WEEK_SALE_PRICE"));
				re.assign("subsCurRegMultiple", substituteInput.getSubStringArray("SUB_CUR_REG_MULTIPLE"));
				re.assign("subsCurRegPrice", substituteInput.getSubStringArray("SUB_CUR_REG_PRICE"));
				re.assign("subsCurSaleMultiple", substituteInput.getSubStringArray("SUB_CUR_SALE_MULTIPLE"));
				re.assign("subsCurSalePrice", substituteInput.getSubStringArray("SUB_CUR_SALE_PRICE"));
				re.assign("impactFactor", substituteInput.getSubStringArray("IMPACT_FACTOR"));

				logger.debug("R Script path: " + subsAdjRRootPath + subsAdjRSrcPath);
				re.assign("root.dir", subsAdjRRootPath);
				re.assign("log.path", logPath);
				re.eval("source('" + subsAdjRRootPath + subsAdjRSrcPath + "')", false);

				REXP x = re.eval("adjustedMovement");

				logger.debug("x:" + x);

				if (x == null)
					throw new GeneralException("null object returned from R");
				RList l = x.asList();

				double[] mainItemCodes = l.at("mainItemCodes").asDoubleArray();
				double[] mainItemAdjRegPred = l.at("mainItemAdjRegPred").asDoubleArray();
				double[] mainItemAdjSalePred = l.at("mainItemAdjSalePred").asDoubleArray();

				for (int i = 0; i < mainItemCodes.length; i++) {
					SubstituteAdjROutputDTO subsOutput = new SubstituteAdjROutputDTO();
					subsOutput.setItemKey(new ItemKey((int) mainItemCodes[i], PRConstants.NON_LIG_ITEM_INDICATOR));
					subsOutput.setAdjRegMov(mainItemAdjRegPred[i]);
					subsOutput.setAdjSaleMov(mainItemAdjSalePred[i]);
					substituteAdjROutputDTO.add(subsOutput);
				}
			} else {
				logger.info("No active substitute items");
			}
			
			//only for debugging

//			for (SubjstituteAdjRMainItemDTO subsAdjMainItemDTO : substituteInput.getMainItems()) {
//				SubstituteAdjROutputDTO subsOutput = new SubstituteAdjROutputDTO();
//				subsOutput.setItemKey(new ItemKey(subsAdjMainItemDTO.getItemKey().getItemCodeOrRetLirId(), PRConstants.NON_LIG_ITEM_INDICATOR));
//				subsOutput.setAdjRegMov(subsAdjMainItemDTO.getRegPrediction() - (subsAdjMainItemDTO.getRegPrediction() * 0.1));
//				// subsOutput.setAdjSaleMov(subsAdjMainItemDTO.getSalePrediction()
//				// - (subsAdjMainItemDTO.getSalePrediction() * 0.1));
//				substituteAdjROutputDTO.add(subsOutput);
//			}
			
		} catch (Exception e) {
			logger.error(e.toString());
			throw new GeneralException(e.toString());
		}

		return substituteAdjROutputDTO;
	}

	private void logInputValues(SubstituteAdjRInputDTO substituteInput) {
		logger.debug("mainItemCodes = " + PRCommonUtil.getCommaSeperatedStringFromInt(substituteInput.getIntArray("MAIN_ITEM_CODE")));
		logger.debug("mainItemRecWeekRegMultiple = "
				+ PRCommonUtil.getCommaSeperatedStringFromInt(substituteInput.getIntArray("MAIN_ITEM_REC_WEEK_REG_MULTIPLE")));
		logger.debug("mainItemRecWeekRegPrice = "
				+ PRCommonUtil.getCommaSeperatedStringFromDouble(substituteInput.getDoubleArray("MAIN_ITEM_REC_WEEK_REG_PRICE")));
		logger.debug("mainItemRecWeekSaleMultiple = "
				+ PRCommonUtil.getCommaSeperatedStringFromInt(substituteInput.getIntArray("MAIN_ITEM_REC_WEEK_SALE_MULTIPLE")));
		logger.debug("mainItemRecWeekSalePrice = "
				+ PRCommonUtil.getCommaSeperatedStringFromDouble(substituteInput.getDoubleArray("MAIN_ITEM_REC_WEEK_SALE_PRICE")));
		logger.debug("mainItemRegPred = " + PRCommonUtil.getCommaSeperatedStringFromDouble(substituteInput.getDoubleArray("MAIN_ITEM_REG_PRED")));
		logger.debug("mainItemSalePred = " + PRCommonUtil.getCommaSeperatedStringFromDouble(substituteInput.getDoubleArray("MAIN_ITEM_SALE_PRED")));
		logger.debug("subsItemCodes = " + PRCommonUtil.getCommaSeperatedStringFromString(substituteInput.getSubStringArray("SUB_ITEM_CODE"), true));
		logger.debug("subsLevelId = " + PRCommonUtil.getCommaSeperatedStringFromString(substituteInput.getSubStringArray("SUB_LEVEL_ID"), true));
		logger.debug("subsRecWeekRegMultiple = "
				+ PRCommonUtil.getCommaSeperatedStringFromString(substituteInput.getSubStringArray("SUB_REC_WEEK_REG_MULTIPLE"), true));
		logger.debug("subsRecWeekRegPrice = "
				+ PRCommonUtil.getCommaSeperatedStringFromString(substituteInput.getSubStringArray("SUB_REC_WEEK_REG_PRICE"), true));
		logger.debug("subsRecWeekSaleMultiple = "
				+ PRCommonUtil.getCommaSeperatedStringFromString(substituteInput.getSubStringArray("SUB_REC_WEEK_SALE_MULTIPLE"), true));
		logger.debug("subsRecWeekSalePrice = "
				+ PRCommonUtil.getCommaSeperatedStringFromString(substituteInput.getSubStringArray("SUB_REC_WEEK_SALE_PRICE"), true));
		logger.debug("subsCurRegMultiple = "
				+ PRCommonUtil.getCommaSeperatedStringFromString(substituteInput.getSubStringArray("SUB_CUR_REG_MULTIPLE"), true));
		logger.debug(
				"subsCurRegPrice = " + PRCommonUtil.getCommaSeperatedStringFromString(substituteInput.getSubStringArray("SUB_CUR_REG_PRICE"), true));
		logger.debug("subsCurSaleMultiple = "
				+ PRCommonUtil.getCommaSeperatedStringFromString(substituteInput.getSubStringArray("SUB_CUR_SALE_MULTIPLE"), true));
		logger.debug("subsCurSalePrice = "
				+ PRCommonUtil.getCommaSeperatedStringFromString(substituteInput.getSubStringArray("SUB_CUR_SALE_PRICE"), true));
		logger.debug("impactFactor = " + PRCommonUtil.getCommaSeperatedStringFromString(substituteInput.getSubStringArray("IMPACT_FACTOR"), true));
	}
}
