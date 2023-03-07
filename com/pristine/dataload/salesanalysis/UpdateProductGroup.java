package com.pristine.dataload.salesanalysis;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

import javax.sql.rowset.CachedRowSet;

import com.pristine.dao.DBManager;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class UpdateProductGroup {

	public static void main(String[] args) throws GeneralException,
			SQLException {

		Connection conn = null;

		PropertyManager.initialize("analysis.properties");

		// get the db connection

		conn = DBManager.getConnection();
		conn.setAutoCommit(true);

		int a = Integer.valueOf(args[0]);

		updateProductGroup(conn, a);

	}

	private static void updateProductGroup(Connection conn, int a)
			throws GeneralException, SQLException {

		StringBuffer query = new StringBuffer();

		StringBuffer query2 = new StringBuffer();

		HashMap<String, String> prodcutMap = new HashMap<String, String>();

		HashMap<String, String> itemMap = new HashMap<String, String>();

		query.append(" select distinct CATEGORY_ID as CATEGORYID from ITEM_LOOKUP  ");

		CachedRowSet result = PristineDBUtil.executeQuery(conn, query,
				"XXXXXXXXXXXXXXXX");

		while (result.next()) {

			itemMap.put(result.getString("CATEGORYID"),
					result.getString("CATEGORYID"));

		}

		query2.append(" select CHILD_PRODUCT_ID from PRODUCT_GROUP_RELATION where PRODUCT_LEVEL_ID=8  ");

		CachedRowSet result1 = PristineDBUtil.executeQuery(conn, query2,
				"XXXXXXXXXXXXXXXX");

		while (result1.next()) {

			prodcutMap.put(result1.getString("CHILD_PRODUCT_ID"),
					result1.getString("CHILD_PRODUCT_ID"));
		}

		Object[] productArray = itemMap.values().toArray();
		for (int wM = 0; wM < productArray.length; wM++) {
			if (prodcutMap.containsKey(productArray[wM])) {
				System.out.println("Already Updated");
			} else {
				StringBuffer query3 = new StringBuffer();

				query3.append(" insert into PRODUCT_GROUP_RELATION (product_id,child_product_id,child_product_level_id,product_level_id) ");
				query3.append(" values (211,'" + productArray[wM] + "',4,8) ");

				System.out.println("Sql:" + query3.toString());

				PristineDBUtil.executeUpdate(conn, query3,
						"&&&&&&&&&&&&&&&&&&&&&");

			}

		}

	}

}
