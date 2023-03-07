package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.ListIterator;
import com.pristine.dto.SuspectItemDTO;
import com.pristine.dto.SuspectItemReasonDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.GenericUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.SIApplicationInterface;

public class SuspectItemDAO
{
	// private static Logger logger = Logger.getLogger(InsertSIDAO.class);

	public static void clearSuspectItemData (SIApplicationInterface siApp)
		throws GeneralException
	{
		Connection conn = siApp.getConnection();
		
		try
		{
			conn.setAutoCommit(false);
			
			// Clear existing entries, if any, for the schedule 
			int schId = siApp.getScheduleId();
			siApp.getRulesDB().clearSuspectData(schId);				// clear suspect table entries
			siApp.getCompetitiveDataDB().clearSuspectFlag (schId);	// also clear comp_data suspect flag
			
			PristineDBUtil.commitTransaction(conn, "Suspect Item clear commit");
		}
		catch (Exception ex)
		{
			GenericUtil.logError(ex.getMessage(), ex);
			PristineDBUtil.rollbackTransaction(conn, "Suspect Item clear failed");
		}
	}

	public static void insertSuspectItemsInDB(SIApplicationInterface siApp, ArrayList<SuspectItemDTO> suspectItemDTOs) throws GeneralException
	{
		Connection conn = siApp.getConnection();
		PreparedStatement prepraredStmt = null;

		try
		{
			conn.setAutoCommit(false);

			StringBuffer insertBuffer = new StringBuffer();

			insertBuffer.append("insert into suspect_item(check_data_id, item_id, schedule_id, ");
			insertBuffer.append(" comp_str_id, suspect_reason_id, suspect_comments, suggested_price, ");
			insertBuffer.append(" suggested_qty, suggested_reason_id, sale_ind, isremoved) values");
			insertBuffer.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

			prepraredStmt = conn.prepareStatement(insertBuffer.toString());

			ArrayList<Integer> itemCodeList = new ArrayList<Integer>();
			ListIterator<SuspectItemDTO> suspectItemList = suspectItemDTOs.listIterator();
			while (suspectItemList.hasNext())
			{
				SuspectItemDTO suspectItemDTO = suspectItemList.next();

				prepraredStmt.setInt(1, suspectItemDTO.getCheck_Data_ID());
				prepraredStmt.setInt(2, suspectItemDTO.getItem_ID());
				prepraredStmt.setInt(3, suspectItemDTO.getSchedule_ID());
				prepraredStmt.setInt(4, suspectItemDTO.getComp_Str_ID());
				prepraredStmt.setInt(5, 0);
				insertSuspectReasons(suspectItemDTO.getSuspectReasonDTOs(), conn);
				prepraredStmt.setString(6, suspectItemDTO.getSuspect_Comments());
				prepraredStmt.setFloat(7, suspectItemDTO.getSuggested_Price());
				prepraredStmt.setFloat(8, suspectItemDTO.getSuggested_Qty());
				prepraredStmt.setInt(9, suspectItemDTO.getSuggested_Reason_ID());
				//prepraredStmt.setInt(10, suspectItemDTO.getApproval_Status());
				prepraredStmt.setString(10, suspectItemDTO.getSale_Ind().toString());
				prepraredStmt.setString(11, suspectItemDTO.isRemoved() ? "Y" : "N");

				prepraredStmt.execute();
				
				if ( !suspectItemDTO.isRemoved() ) {
					itemCodeList.add (suspectItemDTO.getItem_ID());
				}
			}
			
			// Update comp_data suspect flag
			siApp.getCompetitiveDataDB().setSuspectFlag (siApp.getScheduleId(), itemCodeList);
			
			PristineDBUtil.commitTransaction(conn, "Suspect Item insert commit");
		}
		catch (Exception ex)
		{
			GenericUtil.logError(ex.getMessage(), ex);
			PristineDBUtil.rollbackTransaction(conn, "Suspect Item insert failed");
		}
		finally
		{
			PristineDBUtil.close(prepraredStmt);
		}
	}

	public static void insertSuspectReasons(ArrayList<SuspectItemReasonDTO> suspectReasonDTOs, Connection connection) throws GeneralException
	{
		PreparedStatement prepraredStmt = null;

		try
		{

			StringBuffer insertBuffer = new StringBuffer();

			insertBuffer.append("insert into suspect_item_reasons(CHECK_DATA_ID, SUSPECT_REASON_ID, SUSPECT_COMMENTS, suspect_reason_details, item_removal_details, price_suggestion_details, isremoved, isForSale) values ");
			insertBuffer.append("(?, ?, ?, ?, ?, ?, ?, ?)");

			prepraredStmt = connection.prepareStatement(insertBuffer.toString());

			ListIterator<SuspectItemReasonDTO> suspectReasonList = suspectReasonDTOs.listIterator();

			while (suspectReasonList.hasNext())
			{
				SuspectItemReasonDTO suspectReasonDTO = suspectReasonList.next();

				prepraredStmt.setInt(1, suspectReasonDTO.getCheckDataID());
				prepraredStmt.setInt(2, suspectReasonDTO.getReasonID());
				prepraredStmt.setString(3, suspectReasonDTO.getComments());
				prepraredStmt.setString(4, suspectReasonDTO.getSuspectDetails());
				prepraredStmt.setString(5, suspectReasonDTO.getRemovalDetails());
				prepraredStmt.setString(6, suspectReasonDTO.getPriceSuggestionDetails());
				prepraredStmt.setString(7, suspectReasonDTO.getIsRemoved().toString());
				prepraredStmt.setString(8, suspectReasonDTO.getIsForSale().toString());

				prepraredStmt.executeUpdate();
			}
		}
		catch (Exception ex)
		{
			GenericUtil.logError(ex.getMessage(), ex);
		}
		finally
		{
			PristineDBUtil.close(prepraredStmt);
		}
	}
}
