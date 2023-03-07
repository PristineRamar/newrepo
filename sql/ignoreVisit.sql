--0108
update summary_daily_mov_iv s set s.sixty_four_exp = 
(select pred from sixty_four_exp e 
where s.item_code = e.item_code and e.comp_str_id = 5651 and to_date(pred_date,'yyyy-MM-dd') = to_date('06/18/2013','MM/dd/yyyy')
)
where s.tod_id = 4  and s.schedule_id = 6294 and s.dow_id = 3



--===FOCUS ITEM GROUP

--0108
update summary_daily_mov_iv s set s.sixty_four_exp = 
(select pred from sixty_four_exp_focus e 
where s.item_code = e.item_code and e.comp_str_id = 5651 and to_date(pred_date,'yyyy-MM-dd') = to_date('06/18/2013','MM/dd/yyyy')
), focus_item = 'Y'
where s.tod_id = 4  and s.schedule_id = 6294 and s.dow_id = 3
and item_code in(select item_code from sixty_four_exp_focus e 
where comp_str_id = 5651 and  to_date(pred_date,'yyyy-MM-dd') = to_date('06/18/2013','MM/dd/yyyy'))
