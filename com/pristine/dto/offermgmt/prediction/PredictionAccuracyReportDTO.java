package com.pristine.dto.offermgmt.prediction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.pristine.dto.offermgmt.oos.OOSItemDTO;

public class PredictionAccuracyReportDTO {
	public String FROM_DATE;
	public String TO_DATE;
	public int TOTAL_ITEMS;
	public int TOTAL_ITEMS_DATA;
	public int NO_OF_ITEMS_OR_BLOCKS_CONSIDERED;
	public int ACT_MOV_LT_X1_UNITS;
	public int ACT_MOV_ZERO;
	public int ACT_MOV_LT_X2_UNITS;
	public int ACT_MOV_GT_X1_UNITS;
	public int ACT_MOV_BT_X1_X2_UNITS;
	public int ACT_MOV_LT_X3_UNITS;
	public double RMSE_PRESTO;
	public double RMSE_CLIENT;
	public double RMSE_HIGH_MOVER_PRESTO;
	public double RMSE_HIGH_MOVER_CLIENT;
	public double RMSE_MEDIUM_MOVER_PRESTO;
	public double RMSE_MEDIUM_MOVER_CLIENT;
	public double RMSE_SLOW_MOVER_PRESTO;
	public double RMSE_SLOW_MOVER_CLIENT;
	public double AVG_ERR_PCT_PRESTO;
	public double AVG_ERR_PCT_CLIENT;
	public double AVG_ERR_HIGH_MOVER_PCT_PRESTO;
	public double AVG_ERR_HIGH_MOVER_PCT_CLIENT;
	public double AVG_ERR_MEDIUM_MOVER_PCT_PRESTO;
	public double AVG_ERR_MEDIUM_MOVER_PCT_CLIENT;
	public double AVG_ERR_SLOW_MOVER_PCT_PRESTO;
	public double AVG_ERR_SLOW_MOVER_PCT_CLIENT;
	public double FORCST_CLOSER_PRESTO;
	public double FORCST_CLOSER_PCT_PRESTO;
	public double FORCST_CLOSER_CLIENT;
	public double FORCST_CLOSER_PCT_CLIENT;
	public double FORCST_HIGHER_PRESTO;
	public double FORCST_HIGHER_PCT_PRESTO;
	public double FORCST_HIGHER_CLIENT;
	public double FORCST_HIGHER_PCT_CLIENT;
	public double AVG_HIGHER_FORCST_PCT_PRESTO;
	public double AVG_HIGHER_FORCST_PCT_CLIENT;
	public double FORCST_LOWER_PRESTO;
	public double FORCST_LOWER_PCT_PRESTO;
	public double FORCST_LOWER_CLIENT;
	public double FORCST_LOWER_PCT_CLIENT;
	public double AVG_LOWER_FORCST_PCT_PRESTO;
	public double AVG_LOWER_FORCST_PCT_CLIENT;
	public double FORCST_GT_ACT_X1_TIMES_PRESTO;
	public double FORCST_GT_ACT_X1_TIMES_PCT_PRESTO;
	public double FORCST_GT_ACT_X1_TIMES_CLIENT;
	public double FORCST_GT_ACT_X1_TIMES_PCT_CLIENT;
	public double FORCST_LT_ACT_X1_TIMES_PRESTO;
	public double FORCST_LT_ACT_X1_TIMES_PCT_PRESTO;
	public double FORCST_LT_ACT_X1_TIMES_CLIENT;
	public double FORCST_LT_ACT_X1_TIMES_PCT_CLIENT;
	public double FORCST_WITHIN_X1_PCT_TOT_PRESTO;
	public double FORCST_WITHIN_X1_PCT_PCT_PRESTO;
	public double FORCST_WITHIN_X1_PCT_TOT_CLIENT;
	public double FORCST_WITHIN_X1_PCT_PCT_CLIENT;
	public double MAX_FORCST_INCOR_PCT_PRESTO;
	public double MAX_FORCST_INCOR_PCT_CLIENT;
	public double NO_FORCST_ITEMS_PRESTO;
	public double NO_FORCST_ITEMS_PCT_PRESTO;
	public double NO_FORCST_ITEMS_CLIENT;
	public double NO_FORCST_ITEMS_PCT_CLIENT;
	public double BOTH_FORCST_HIGHER;
	public double BOTH_FORCST_HIGHER_PCT;
	public double BOTH_FORCST_LOWER;
	public double BOTH_FORCST_LOWER_PCT;
	public int NO_OF_ITEMS_OR_BLOCKS_IGNORED;
	
	/**** Detail Sheet Properties ***/
	public int totNoOfAdCategories = 0;
	public int totNoOfAdItems = 0;
	public int totNoOfLIGs = 0;
	public int totNoOfNonLIGs = 0;
	
	public int totNoOfNonPerishablesAdCategories =0;
	public int totNoOfNonPerishablesAdItems=0;
	public int totNoOfNonPerishablesLIGs=0;
	public int totNoOfNonPerishablesNonLIGs=0;
	
	public int totNoOfNonPerishablesAdCategoriesWPred =0;
	public int totNoOfNonPerishablesAdItemsWPred=0;
	public int totNoOfNonPerishablesLIGsWPred=0;
	public int totNoOfNonPerishablesNonLIGsWPred=0;
	
	public int totNoOfNonPerishablesAdItemsWZeroPred=0;
	public int totNoOfNonPerishablesLIGsWZeroPred=0;
	public int totNoOfNonPerishablesNonLIGsWZeroPred=0;
	
	public int totNoOfNonPerishablesAdCategoriesWOPred =0;
	public int totNoOfNonPerishablesAdItemsWOPred=0;
	public int totNoOfNonPerishablesLIGsWOPred=0;
	public int totNoOfNonPerishablesNonLIGsWOPred=0;
	
	public int totNoOfNonPerishablesWith1LIGor1NonLigAdCategories =0;
	public int totNoOfNonPerishablesWith1LIGor1NonLigAdItems=0;
	public int totNoOfNonPerishablesWith1LIGor1NonLigLIGs=0;
	public int totNoOfNonPerishablesWith1LIGor1NonLigNonLIGs=0;
	public int totNoOfNonPerishablesWith1LIGor1NonLigAdCategoriesWPred = 0;
	public int totNoOfNonPerishablesWith1LIGor1NonLigAdCategoriesWoPred = 0;
	
	public HashMap<String, Integer> totNoOfNonPerishablesCategoriesWOPrediction = new HashMap<String, Integer>();
	public List<OOSItemDTO> inAccurateItems = new ArrayList<>();
	public List<OOSItemDTO> inAccurateItemsByAbsError = new ArrayList<>();
}
