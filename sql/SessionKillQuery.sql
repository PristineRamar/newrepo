alter system kill session 'sid,serial#' immediate;
select s.sid, s.serial#, p.spid, s.username, s.schemaname
     , s.program, s.terminal, s.osuser
  from v$session s
  join v$process p
    ON S.PADDR = P.ADDR
 where s.type != 'BACKGROUND';