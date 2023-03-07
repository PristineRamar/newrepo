select count(*) from( select * from retail_calendar dc, (select rc.start_date sd, rc.end_date ed from schedule s1, retail_calendar rc where s1.comp_str_id = 5670 and s1.start_date = rc.start_date and rc.calendar_id in ((select calendar_id from retail_calendar where ((start_date >= to_date('06/06/2013','MM/dd/yyyy') - 120 and end_date <= to_date('06/06/2013','MM/dd/yyyy')-1) or (start_date <= to_date('06/06/2013','MM/dd/yyyy') - 120 and end_date >= to_date('06/06/2013','MM/dd/yyyy')-120) or (start_date <= to_date('06/06/2013','MM/dd/yyyy') -1 and end_date >= to_date('06/06/2013','MM/dd/yyyy')-1)) and row_type = 'W') ) and s1.schedule_id not in  (select  distinct s.schedule_id from summary_daily_mov s, schedule ss, retail_calendar rc where ((s.promotional_ind = 'Y' and s.unit_sale_price = 2.0) or (s.promotional_ind = 'N' and s.unit_reg_price = 2.0)) and  s.schedule_id = ss.schedule_id and ss.comp_str_id = 5670 and s.tod_id = 4 and ss.start_date = rc.start_date and rc.row_type = 'W' and( (rc.start_date >= to_date('06/06/2013','MM/dd/yyyy')-120 and rc.end_date <= to_date('06/06/2013','MM/dd/yyyy')-1 ) or (rc.start_date <= to_date('06/06/2013','MM/dd/yyyy')-1 and rc.end_date >= to_date('06/06/2013','MM/dd/yyyy')-1 ) or (rc.start_date <= to_date('06/06/2013','MM/dd/yyyy')-120 and rc.end_date >= to_date('06/06/2013','MM/dd/yyyy')-120 )) and item_code = 835108 )) TabA where dc.start_date >= tabA.sd and dc.start_date  <= tabA.ed and dc.start_date >= to_date('06/06/2013','MM/dd/yyyy')-120 and dc.start_date <= to_date('06/06/2013','MM/dd/yyyy')-1 and dc.row_type = 'D')

select count(*) from
( select * from retail_calendar dc, 
(select rc.start_date sd, rc.end_date ed from schedule s1, retail_calendar rc where s1.comp_str_id = 5670 
and s1.start_date = rc.start_date and rc.calendar_id in 
((select calendar_id from retail_calendar 
where ((start_date >= to_date('06/06/2013','MM/dd/yyyy') - 42 and end_date <= to_date('06/06/2013','MM/dd/yyyy')-1) 
or (start_date <= to_date('06/06/2013','MM/dd/yyyy') - 42 and end_date >= to_date('06/06/2013','MM/dd/yyyy')-42) 
or (start_date <= to_date('06/06/2013','MM/dd/yyyy') -1 and end_date >= to_date('06/06/2013','MM/dd/yyyy')-1)) and row_type = 'W') ) 
and s1.schedule_id not in  
(select  distinct s.schedule_id from summary_daily_mov s, schedule ss, retail_calendar rc 
where 
--((s.promotional_ind = 'Y' and s.unit_sale_price = 2.0) or (s.promotional_ind = 'N' and s.unit_reg_price = 2.0)) and  
s.schedule_id = ss.schedule_id and ss.comp_str_id = 5670 and s.tod_id = 4 and ss.start_date = rc.start_date and rc.row_type = 'W' 
and( (rc.start_date >= to_date('06/06/2013','MM/dd/yyyy')-42 and rc.end_date <= to_date('06/06/2013','MM/dd/yyyy')-1 ) 
or (rc.start_date <= to_date('06/06/2013','MM/dd/yyyy')-1 and rc.end_date >= to_date('06/06/2013','MM/dd/yyyy')-1 ) 
or (rc.start_date <= to_date('06/06/2013','MM/dd/yyyy')-42 and rc.end_date >= to_date('06/06/2013','MM/dd/yyyy')-42 )) 
and item_code = 835108 )) TabA where dc.start_date >= tabA.sd and dc.start_date  <= tabA.ed and dc.start_date >= to_date('06/06/2013','MM/dd/yyyy')-42 
and dc.start_date <= to_date('06/06/2013','MM/dd/yyyy')-1 and dc.row_type = 'D')

