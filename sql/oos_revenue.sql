Select comp_str_no, analysis_date, sum(oos_revenue) from
(select comp_str_no, start_date + dow_id-1 analysis_date, (exp_used- quantity_total- sigma_used)* unit_price oos_revenue from 
(select tod_id,schedule_id, dow_id, quantity_total, oos_method, sigma_used,
case
when oos_method = 'last 7 days' then last_7day_exp
when oos_method = 'last 28 days' then last_28day_exp
when oos_method = 'last 120 days' then last_180day_exp
when oos_method = 'last 42 days avg' then last_7day_avg_exp
else 0
end exp_used,
(case when promotional_ind = 'Y' then unit_sale_price else unit_reg_price  end) unit_price,
last_7day_exp, last_28day_exp, last_180day_exp, last_7day_avg_exp , sixty_four_exp
from summary_daily_mov
where tod_id = 4 and OOS_IND = 'Y'--and schedule_id in (select schedule_id from schedule where comp_str_id = &&store_id)
) TabA
, schedule s, competitor_store cstore
where TabA.schedule_id = s.schedule_id
and s.comp_str_id = cstore.comp_str_id
and s.start_date >= to_date('06/09/2013','MM/dd/yyyy'))
group by  comp_str_no, analysis_date
order by  comp_str_no, analysis_date


Select comp_str_no, analysis_date, sum(oos_revenue) from
(select comp_str_no, start_date + dow_id-1 analysis_date, (exp_used- quantity_total- sigma_used)* unit_price oos_revenue from 
(select tod_id,schedule_id, dow_id, quantity_total, oos_method, sigma_used,
case
when oos_method = 'last 7 days' then last_7day_exp
when oos_method = 'last 28 days' then last_28day_exp
when oos_method = 'last 120 days' then last_180day_exp
when oos_method = 'last 42 days avg' then last_7day_avg_exp
else 0
end exp_used,
(case when promotional_ind = 'Y' then unit_sale_price else unit_reg_price  end) unit_price,
last_7day_exp, last_28day_exp, last_180day_exp, last_7day_avg_exp , sixty_four_exp
from summary_daily_mov
where tod_id <> 4 and OOS_IND = 'Y'--and schedule_id in (select schedule_id from schedule where comp_str_id = &&store_id)
) TabA
, schedule s, competitor_store cstore
where TabA.schedule_id = s.schedule_id
and s.comp_str_id = cstore.comp_str_id
and s.start_date >= to_date('08/01/2013','MM/dd/yyyy'))
group by  comp_str_no, analysis_date
order by  comp_str_no, analysis_date