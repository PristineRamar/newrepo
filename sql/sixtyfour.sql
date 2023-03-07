--duplicates elimination in sixty four exp
delete  from predicted_exp s
where (s.item_code,s.comp_str_id,s.pred_date) in 
(select item_code c1, comp_str_id c2, pred_date  from
(select item_code, comp_str_id, pred_date, count(pred) cc from predicted_exp
group by pred_date, comp_str_id, item_code)
where cc > 1
) 


update predicted_exp s set s.item_code = (select l.item_code from item_lookup l where l.upc =  to_char(s.upc,'FM000000000000')) 
where s.item_code is null

update predicted_exp set comp_str_id = 5651 where comp_str_no = '108' and comp_str_id is null
update predicted_exp set comp_str_id = 5673 where comp_str_no = '233' and comp_str_id is null


--0108
update summary_daily_mov s set s.sixty_four_exp = 
(select pred from predicted_exp e 
where s.item_code = e.item_code and e.comp_str_id = 5651 and to_date(pred_date,'yyyy-MM-dd') = to_date('07/17/2013','MM/dd/yyyy')
)
where s.tod_id = 4  and s.schedule_id = 6366 and s.dow_id = 4

--0233
update summary_daily_mov s set s.sixty_four_exp = 
(select pred from predicted_exp e 
where s.item_code = e.item_code and e.comp_str_id = 5673 and to_date(pred_date,'yyyy-MM-dd') = to_date('07/17/2013','MM/dd/yyyy')
)
where  s.tod_id =4 and s.schedule_id = 6363 and s.dow_id = 4


--===FOCUS ITEM GROUP

update predicted_exp_focus s set s.item_code = (select l.item_code from item_lookup l where l.upc =  to_char(s.upc,'FM000000000000')) 
where s.item_code is null

update predicted_exp_focus set comp_str_id = 5651 where comp_str_no = '108' and comp_str_id is null
update predicted_exp_focus set comp_str_id = 5673 where comp_str_no = '233' and comp_str_id is null

delete  from predicted_exp_focus s
where (s.item_code,s.comp_str_id,s.pred_date) in 
(select item_code c1, comp_str_id c2, pred_date  from
(select item_code, comp_str_id, pred_date, count(pred) cc from predicted_exp_focus
group by pred_date, comp_str_id, item_code)
where cc > 1
) 


--0108
update summary_daily_mov s set s.sixty_four_exp = 
(select pred from predicted_exp_focus e 
where s.item_code = e.item_code and e.comp_str_id = 5651 and to_date(pred_date,'yyyy-MM-dd') = to_date('07/17/2013','MM/dd/yyyy')
), focus_item = 'Y'
where s.tod_id = 4  and s.schedule_id = 6366 and s.dow_id = 4
and item_code in(select item_code from predicted_exp_focus e 
where comp_str_id = 5651 and  to_date(pred_date,'yyyy-MM-dd') = to_date('07/17/2013','MM/dd/yyyy'))

--0233
update summary_daily_mov s set s.sixty_four_exp = 
(select pred from predicted_exp_focus e 
where s.item_code = e.item_code and e.comp_str_id = 5673 and to_date(pred_date,'yyyy-MM-dd') = to_date('07/17/2013','MM/dd/yyyy')
), focus_item = 'Y'
where  s.tod_id =4 and s.schedule_id = 6363 and s.dow_id = 4
and item_code in(select item_code from predicted_exp_focus e 
where comp_str_id = 5673 and to_date(pred_date,'yyyy-MM-dd') = to_date('07/17/2013','MM/dd/yyyy'))
