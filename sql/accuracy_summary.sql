--define store_id = 5670;
--define store_id = 5651;
--define store_id = 5673;
define store_id = 5704;

select 
--schedule_id,dow_id,
cstore.comp_str_no,s.start_date + dow_id-1 "Analysis date",
round(sum(case when abs(quantity_total - round(exp_used))=0 then 1 else 0 end)/count(*),4) "Act = Pred",
round(sum(case when quantity_total > round(exp_used) then 1 else 0 end)/count(*),4) "Act > Pred",
round(sum(case when quantity_total - round(exp_used)=1 then 1 else 0 end)/count(*),4) "Act - Pred = 1",
round(sum(case when quantity_total - round(exp_used)=2 then 1 else 0 end)/count(*),4) "Act - Pred = 2",
round(sum(case when quantity_total - round(exp_used)>2 then 1 else 0 end)/count(*),4) "Act - Pred > 2",
round(sum(case when round(exp_used) > quantity_total  then 1 else 0 end)/count(*),4) "Pred > Act",
round(sum(case when round(exp_used) - quantity_total =1 then 1 else 0 end)/count(*),4) "Pred - Act = 1",
round(sum(case when round(exp_used) - quantity_total =2 then 1 else 0 end)/count(*),4) "Pred - Act = 2",
round(sum(case when round(exp_used) - quantity_total >2 then 1 else 0 end)/count(*),4) "Pred - Act > 2",
round(sum(case when abs(quantity_total - round(last_7day_exp))=0 then 1 else 0 end)/count(*),4) "7 DAY : Act = Pred",
round(sum(case when quantity_total > round(last_7day_exp) then 1 else 0 end)/count(*),4) "7 DAY : Act > Pred",
round(sum(case when quantity_total - round(last_7day_exp)=1 then 1 else 0 end)/count(*),4) "7 DAY : Act - Pred = 1",
round(sum(case when quantity_total - round(last_7day_exp)=2 then 1 else 0 end)/count(*),4) "7 DAY : Act - Pred = 2",
round(sum(case when quantity_total - round(last_7day_exp)>2 then 1 else 0 end)/count(*),4) "7 DAY : Act - Pred > 2",
round(sum(case when round(last_7day_exp) > quantity_total  then 1 else 0 end)/count(*),4) "7 DAY : Pred > Act",
round(sum(case when round(last_7day_exp) - quantity_total =1 then 1 else 0 end)/count(*),4) "7 DAY : Pred - Act = 1",
round(sum(case when round(last_7day_exp) - quantity_total =2 then 1 else 0 end)/count(*),4) "7 DAY : Pred - Act = 2",
round(sum(case when round(last_7day_exp) - quantity_total >2 then 1 else 0 end)/count(*),4) "7 DAY : Pred - Act > 2",
round(sum(case when abs(quantity_total - round(last_28day_exp))=0 then 1 else 0 end)/count(*),4) "28 DAY : Act = Pred",
round(sum(case when quantity_total > round(last_28day_exp) then 1 else 0 end)/count(*),4) "28 DAY : Act > Pred",
round(sum(case when quantity_total - round(last_28day_exp)=1 then 1 else 0 end)/count(*),4) "28 DAY : Act - Pred = 1",
round(sum(case when quantity_total - round(last_28day_exp)=2 then 1 else 0 end)/count(*),4) "28 DAY : Act - Pred = 2",
round(sum(case when quantity_total - round(last_28day_exp)>2 then 1 else 0 end)/count(*),4) "28 DAY : Act - Pred > 2",
round(sum(case when round(last_28day_exp) > quantity_total  then 1 else 0 end)/count(*),4) "28 DAY : Pred > Act",
round(sum(case when round(last_28day_exp) - quantity_total =1 then 1 else 0 end)/count(*),4) "28 DAY : Pred - Act = 1",
round(sum(case when round(last_28day_exp) - quantity_total =2 then 1 else 0 end)/count(*),4) "28 DAY : Pred - Act = 2",
round(sum(case when round(last_28day_exp) - quantity_total >2 then 1 else 0 end)/count(*),4) "28 DAY : Pred - Act > 2",
round(sum(case when abs(quantity_total - round(last_180day_exp))=0 then 1 else 0 end)/count(*),4) "120 DAY : Act = Pred",
round(sum(case when quantity_total > round(last_180day_exp) then 1 else 0 end)/count(*),4) "120 DAY : Act > Pred",
round(sum(case when quantity_total - round(last_180day_exp)=1 then 1 else 0 end)/count(*),4) "120 DAY : Act - Pred = 1",
round(sum(case when quantity_total - round(last_180day_exp)=2 then 1 else 0 end)/count(*),4) "120 DAY : Act - Pred = 2",
round(sum(case when quantity_total - round(last_180day_exp)>2 then 1 else 0 end)/count(*),4) "120 DAY : Act - Pred > 2",
round(sum(case when round(last_180day_exp) > quantity_total  then 1 else 0 end)/count(*),4) "120 DAY : Pred > Act",
round(sum(case when round(last_180day_exp) - quantity_total =1 then 1 else 0 end)/count(*),4) "120 DAY : Pred - Act = 1",
round(sum(case when round(last_180day_exp) - quantity_total =2 then 1 else 0 end)/count(*),4) "120 DAY : Pred - Act = 2",
round(sum(case when round(last_180day_exp) - quantity_total >2 then 1 else 0 end)/count(*),4) "120 DAY : Pred - Act > 2",
round(sum(case when abs(quantity_total - round(last_7day_avg_exp))=0 then 1 else 0 end)/count(*),4) "42 DAY : Act = Pred",
round(sum(case when quantity_total > round(last_7day_avg_exp) then 1 else 0 end)/count(*),4) "42 DAY : Act > Pred",
round(sum(case when quantity_total - round(last_7day_avg_exp)=1 then 1 else 0 end)/count(*),4) "42 DAY : Act - Pred = 1",
round(sum(case when quantity_total - round(last_7day_avg_exp)=2 then 1 else 0 end)/count(*),4) "42 DAY : Act - Pred = 2",
round(sum(case when quantity_total - round(last_7day_avg_exp)>2 then 1 else 0 end)/count(*),4) "42 DAY : Act - Pred > 2",
round(sum(case when round(last_7day_avg_exp) > quantity_total  then 1 else 0 end)/count(*),4) "42 DAY : Pred > Act",
round(sum(case when round(last_7day_avg_exp) - quantity_total =1 then 1 else 0 end)/count(*),4) "42 DAY : Pred - Act = 1",
round(sum(case when round(last_7day_avg_exp) - quantity_total =2 then 1 else 0 end)/count(*),4) "42 DAY : Pred - Act = 2",
round(sum(case when round(last_7day_avg_exp) - quantity_total >2 then 1 else 0 end)/count(*),4) "42 DAY : Pred - Act > 2",
round(sum(case when abs(quantity_total - round(sixty_four_exp))=0 then 1 else 0 end)/count(*),4) "42 DAY : Act = Pred",
round(sum(case when quantity_total > round(sixty_four_exp) then 1 else 0 end)/count(*),4) "42 DAY : Act > Pred",
round(sum(case when quantity_total - round(sixty_four_exp)=1 then 1 else 0 end)/count(*),4) "42 DAY : Act - Pred = 1",
round(sum(case when quantity_total - round(sixty_four_exp)=2 then 1 else 0 end)/count(*),4) "42 DAY : Act - Pred = 2",
round(sum(case when quantity_total - round(sixty_four_exp)>2 then 1 else 0 end)/count(*),4) "42 DAY : Act - Pred > 2",
round(sum(case when round(sixty_four_exp) > quantity_total  then 1 else 0 end)/count(*),4) "42 DAY : Pred > Act",
round(sum(case when round(sixty_four_exp) - quantity_total =1 then 1 else 0 end)/count(*),4) "42 DAY : Pred - Act = 1",
round(sum(case when round(sixty_four_exp) - quantity_total =2 then 1 else 0 end)/count(*),4) "42 DAY : Pred - Act = 2",
round(sum(case when round(sixty_four_exp) - quantity_total >2 then 1 else 0 end)/count(*),4) "42 DAY : Pred - Act > 2"
from
(select tod_id,schedule_id, dow_id, quantity_total, oos_method,
case
when oos_method = 'last 7 days' then last_7day_exp
when oos_method = 'last 28 days' then last_28day_exp
when oos_method = 'last 120 days' then last_180day_exp
when oos_method = 'last 42 days avg' then last_7day_avg_exp
else 0
end exp_used,
last_7day_exp, last_28day_exp, last_180day_exp, last_7day_avg_exp , sixty_four_exp
from summary_daily_mov
where tod_id = 4 --and schedule_id in (select schedule_id from schedule where comp_str_id = &&store_id)
) TabA, schedule s, competitor_store cstore
where TabA.schedule_id = s.schedule_id
and s.comp_str_id = cstore.comp_str_id
and s.start_date >= to_date('07/21/2013','MM/dd/yyyy')
and dow_id = 5
group by  cstore.comp_str_no, start_date + dow_id-1
order by  cstore.comp_str_no, start_date + dow_id-1
