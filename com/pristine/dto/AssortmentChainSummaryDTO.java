package com.pristine.dto;

import java.util.ArrayList;
import java.util.HashMap;

public class AssortmentChainSummaryDTO implements IValueObject {
	public String deptName;
	public String catName;
	public int itemCount =0;
	public HashMap <String, String> catUniqueStrMap;
	public class GroupInfo {
		public int itemCount =0;
		public ArrayList<Integer> itemList = new ArrayList<Integer>();
		public HashMap <String, String> uniqueStrMap = new HashMap <String, String> ();
		public HashMap <String, String> commonStrMap = new HashMap <String, String> ();
	}
	public GroupInfo PCT_0_5 = new GroupInfo();
	public GroupInfo PCT_5_10 = new GroupInfo();
	public GroupInfo PCT_10_20 = new GroupInfo();
	public GroupInfo PCT_20_35 = new GroupInfo();
	public GroupInfo PCT_35_50 = new GroupInfo();
	public GroupInfo PCT_50_75 = new GroupInfo();
	public GroupInfo PCT_75_90 = new GroupInfo();
	public GroupInfo PCT_90_95 = new GroupInfo();
	public GroupInfo PCT_95_100 = new GroupInfo();
}
