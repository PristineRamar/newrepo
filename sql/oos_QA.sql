--Movement from tlog
select * from movement_daily_oos where item_code = 28481
and to_date(pos_timestamp,'dd-MM-yy') = to_date('06/06/2013','MM/dd/yyyy')
and comp_str_no = 0227

select TOTAL_VISIT_COUNT,	MOV_VISIT_RATIO,	to_date(ss.start_date)+dow_id-1 MOV_DATE,	TOD_ID,	PROMOTIONAL_IND,	UNIT_SALE_PRICE,	UNIT_REG_PRICE,	OOS_IND,	QUANTITY_TOTAL	
 from summary_daily_mov s, schedule ss where s.schedule_id = ss.schedule_id and item_code = 835108
and to_date(ss.start_date)+dow_id-1 >= to_date('04/24/2013','MM/dd/yyyy')
and to_date(ss.start_date)+dow_id-1 <= to_date('06/05/2013','MM/dd/yyyy')
and comp_str_id = 5670 order by ss.start_date + dow_id-1
