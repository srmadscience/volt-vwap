delete from  stocktick_vmap;

exec ReportTick 'TSLA' '2025-04-03 12:20:01' 10 100;
select * from stocktick_vmap;

exec ReportTick 'TSLA' '2025-04-04 12:20:01' 10.05 200;
select * from stocktick_vmap;

exec ReportTick 'TSLA' '2025-04-05 12:20:01' 10.1 300;
select * from stocktick_vmap;

exec ReportTick 'TSLA' '2025-04-05 12:20:02' 10.1 300;
select * from stocktick_vmap;

