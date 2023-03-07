package com.pristine.business.util;

import com.pristine.util.Constants;

public class BusinessHelper {

	// Common function to convert the Units to OZ
    public double ConvertUOMToOZ(int uomId)
    {
        double convertedUnit = 0;

        if (uomId == Constants.UOM_LB)
            convertedUnit = Constants.Unit_Conv_LB2OZ;
        else if (uomId == Constants.UOM_OZ)
            convertedUnit = Constants.Unit_Conv_OZ2OZ;
        else if (uomId == Constants.UOM_ML)
            convertedUnit = Constants.Unit_Conv_ML2OZ;
        else if (uomId == Constants.UOM_QT)
            convertedUnit = Constants.Unit_Conv_QT2OZ;
        else if (uomId == Constants.UOM_GA)
            convertedUnit = Constants.Unit_Conv_GA2OZ;
        else if (uomId == Constants.UOM_PT)
            convertedUnit = Constants.Unit_Conv_PT2OZ;
        else if (uomId == Constants.UOM_GR)
            convertedUnit = Constants.Unit_Conv_GR2OZ;
        else if (uomId == Constants.UOM_LT)
            convertedUnit = Constants.Unit_Conv_LT2OZ;
        else if (uomId == Constants.UOM_FZ)
            convertedUnit = Constants.Unit_Conv_FZ2OZ;
        else if (uomId == Constants.UOM_CUP)
            convertedUnit = Constants.Unit_Conv_CUP2OZ;
        else if (uomId == Constants.UOM_TBLSP)
            convertedUnit = Constants.Unit_Conv_TBLSP2OZ; 
        else if (uomId == Constants.UOM_TSP)
            convertedUnit = Constants.Unit_Conv_TSP2OZ;

        return convertedUnit;
    }
    
    // Common function to convert the Units to OZ
    public double ConvertUOMMinorToOZ(int uomId, int uomMinorId)
    {
        double convertedMajorUnit = 0.0;
        double convertedMinorUnit = 0.0;
        double multiVal = 0;

        if (uomId == uomMinorId)
            convertedMajorUnit = Constants.Unit_Conv_OZ2OZ;
        else
        {
            if (uomId == Constants.UOM_CUP)
                convertedMajorUnit = Constants.Unit_Conv_CUP2OZ;
            else if (uomId == Constants.UOM_TBLSP)
                convertedMajorUnit = Constants.Unit_Conv_TBLSP2OZ;
            else if (uomId == Constants.UOM_TSP)
                convertedMajorUnit = Constants.Unit_Conv_TSP2OZ;
        }

        if (uomMinorId == Constants.UOM_MINOR_3_4)
            multiVal = Constants.UOM_MINOR_VAL_3_4;
        else if (uomMinorId == Constants.UOM_MINOR_2_3)
            multiVal = Constants.UOM_MINOR_VAL_2_3;
        else if (uomMinorId == Constants.UOM_MINOR_1_2)
            multiVal = Constants.UOM_MINOR_VAL_1_2;
        else if (uomMinorId == Constants.UOM_MINOR_1_3)
            multiVal = Constants.UOM_MINOR_VAL_1_3;
        else if (uomMinorId == Constants.UOM_MINOR_1_4)
            multiVal = Constants.UOM_MINOR_VAL_1_4;

        if (multiVal > 0)
            convertedMinorUnit = convertedMajorUnit * multiVal;

        return convertedMinorUnit;
    }
	
    public String FormatIngUOMSize(int UOMId, int UOMMinorId, double size)
    {
        String recipeSizeStr = "";

        if (size > 0)
            recipeSizeStr = recipeSizeStr + size;

        if (UOMMinorId > 0)
        {
            if (recipeSizeStr.length() > 0)
                recipeSizeStr = recipeSizeStr + " ";

            if (UOMMinorId == Constants.UOM_MINOR_1_2)
                recipeSizeStr = recipeSizeStr + "1/2";
            else if (UOMMinorId == Constants.UOM_MINOR_1_3)
                recipeSizeStr = recipeSizeStr + "1/3";
            else if (UOMMinorId == Constants.UOM_MINOR_1_4)
                recipeSizeStr = recipeSizeStr + "1/4";
            else if (UOMMinorId == Constants.UOM_MINOR_2_3)
                recipeSizeStr = recipeSizeStr + "2/3";
            else if (UOMMinorId == Constants.UOM_MINOR_3_4)
                recipeSizeStr = recipeSizeStr + "3/4";
        }

        return recipeSizeStr;
    }
}
