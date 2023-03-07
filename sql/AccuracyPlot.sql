select ss.schedule_id, ss.start_date + dow_id -1, s.quantity_total, s.sixty_four_exp,s.sigma_used,
case
when oos_method = 'last 7 days' then last_7day_exp
when oos_method = 'last 28 days' then last_28day_exp
when oos_method = 'last 120 days' then last_180day_exp
when oos_method = 'last 42 days avg' then last_7day_avg_exp
when oos_method = 'prev day' then prev_day_exp
else 0
end exp_used

from summary_daily_mov s, schedule ss where s.schedule_id = ss.schedule_id and ss. comp_str_id = 5651 and
item_code = 36190
and tod_id = 4
and ss.schedule_id >=6290
order by s.schedule_id , dow_id
