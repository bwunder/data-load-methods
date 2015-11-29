--42825--select count(*) from tActivityHistory
--1214--select count(*) from tReceiveHistory
WITH cteReceive
AS (SELECT ROW_NUMBER() OVER ( PARTITION BY LoadMethod 
                               ORDER BY RecCreateDt) AS Num
         , Id
         , LoadMethod
         , Receive_ms
         , InsertChildRows
         , RecCreateDt
    FROM dbo.tReceiveHistory
    WHERE COALESCE(InsertChildRows,0) > 0)
SELECT REPLACE( REPLACE(receive.LoadMethod, 'View', '')
              , CASE WHEN (ISNUMERIC(RIGHT(receive.LoadMethod, 2)) = 1) 
                     THEN RIGHT(receive.LoadMethod, 2) 
                     WHEN (ISNUMERIC(RIGHT(receive.LoadMethod, 1)) = 1) 
                     THEN RIGHT(receive.LoadMethod, 1) 
                     ELSE '' END
              , '') AS LoadMethod      
     , import.LoadedRows
     , AVG(import.Load_ms + (receive.[Receive_ms/row] * import.LoadedRows)) AS [Load_ms]
     , CASE WHEN (ISNUMERIC(RIGHT(receive.LoadMethod, 2)) = 1) 
            THEN RIGHT(receive.LoadMethod, 2) 
            WHEN (ISNUMERIC(RIGHT(receive.LoadMethod, 1)) = 1) 
            THEN RIGHT(receive.LoadMethod, 1) 
            ELSE 1 END AS ConcurrentLevel 
     , count(*) as count
FROM ( SELECT r1.LoadMethod
            , r1.InsertChildRows
            , r1.Receive_ms*1.0/r1.InsertChildRows AS [Receive_ms/row]
            , r0.BeginDt
            , DATEADD(ms, r1.Receive_ms, r1.RecCreateDT) AS EndDt
       FROM cteReceive r1
       OUTER APPLY (SELECT DATEADD(ms, -1* r1.Receive_ms, MAX(RecCreateDt)) AS BeginDt
                    FROM cteReceive
                    WHERE Id < r1.Id) AS r0) AS receive
JOIN dbo.tActivityHistory import 
ON import.StartDt BETWEEN Receive.BeginDT AND Receive.EndDt
GROUP BY receive.LoadMethod, import.LoadedRows
ORDER BY LoadMethod, LoadedRows, ConcurrentLevel;

/*

SELECT LEFT(a.LoadMethod, LEN(a.LoadMethod) - 1)
FROM target.dbo.tActivityHistory a
JOIN target.dbo.tReceiveHistory r
ON LEFT(a.LoadMethod, LEN(a.LoadMethod) - 1) = r.LoadMethod

select top 5 * FROM target.dbo.tActivityHistory  order by Id desc
select top 5 * FROM  target.dbo.tReceiveHistory order by Id desc


SELECT LoadMethod
     , LoadedRows
     , COUNT(*) AS [test samples] 
FROM target.dbo.tActivityHistory 
GROUP BY LoadMethod, LoadedRows
ORDER BY LoadMethod, LoadedRows




select LoadMethod
     , InsertChildRows
     , count(*) as [test samples] 
from target.dbo.tReceiveHistory 
group by LoadMethod, InsertChildRows
order by LoadMethod, InsertChildRows

SELECT Id
     , CASE WHEN 'AB' THEN 'Views'
            WHEN LEFT(LoadMethod,3) = 'Row' THEN 'Row' 
            ELSE 'TVP' END AS [Load Type]
     , CASE WHEN CHARINDEX('Merge', LoadMethod) > 0 THEN 'Merge' 
            WHEN CHARINDEX('Upsert', LoadMethod) > 0 THEN 'Upsert' END AS [DDL Method]
     , CASE WHEN CHARINDEX('PAG', LoadMethod) > 0
            THEN 'Page' ELSE 'Row' END AS [Locking Level]
     , CASE WHEN CHARINDEX('VER', LoadMethod) > 0
            THEN 'Versions' ELSE 'Locks' END AS [Isolation Type]
     , CASE WHEN ISNUMERIC(RIGHT(LoadMethod, 2)) = 1
            THEN RIGHT(LoadMethod, 2)
            ELSE RIGHT(LoadMethod, 1) END AS [Concurrent Loads]             
     , Load_ms
     , LoadedRows
     , CAST(Load_ms*1.0/ISNULL(NULLIF(LoadedRows,0),-1) AS DECIMAL(12,2)) AS [ms/row]
     , Elapsed_ms
     , StartDT
     , RecCreateDt
FROM target.dbo.tActivityHistory;
--ORDER BY LoadMethod, LoadedRows, [ms/row] DESC;   

--a.RecCreateDt DESC;


DECLARE @sqlhandle VARBINARY(64)
SET @sqlhandle = 0x03000500D093332C8224E800649E00000100000000000000
SELECT * from sys.dm_exec_query_stats
SELECT total_worker_time/execution_count AS [Avg CPU Time],
    SUBSTRING(st.text, (qs.statement_start_offset/2)+1, 
        ((CASE qs.statement_end_offset
          WHEN -1 THEN DATALENGTH(st.text)
         ELSE qs.statement_end_offset
         END - qs.statement_start_offset)/2) + 1) AS statement_text
,qs.*
FROM sys.dm_exec_query_stats AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
WHERE qs.sql_handle = @sqlhandle
ORDER BY total_worker_time/execution_count DESC;
 
 
 -- loadmethod ms/row pivot table
SELECT LoadMethod,[1],[5],[10],[50],[100],[500],[1000],[5000],[10000],[50000],[100000]
FROM ( SELECT LoadMethod, LoadedRows, FLOOR((nullif(Load_ms,0)*1.0/LoadedRows) * 10)/10 AS [ms] 
       FROM target.dbo.tActivityHistory ) AS p
PIVOT
(
 AVG(ms)--COUNT(ms)
FOR LoadedRows IN 
 ([1],[5],[10],[50],[100],[500],[1000],[5000],[10000],[50000],[100000])
) AS pvt
order by LoadMethod 




select 
id, event, source, starttime, endtime
,*
FROM msdb.dbo.sysssislog

select * from 
--delete--update 
target.dbo.tActivityHistory
--set loadmethod = REPLACE(loadmethod, 'LCKPAGLCKPAG','LCKPAG')
where
loadmethod like 'AB%'
--and StartDT < CAST(SYSDATETIME() AS DATE)
--and loadmethod not like 'RowHeap%' 
--LoadedRows not in (1,10,100,1000,10000,100000,5,50,500,5000,50000)
--and 
--and LoadedRows >10000 
order by StartDt--LoadedRows, Load_ms 

select * from 
--delete--update 
target.dbo.tReceiveHistory


use target
--TRUNCATE TABLE dbo.tChild
DELETE dbo.tParent


select session_id
     , login_time
     , total_elapsed_time
     , last_request_end_time
     , reads
     , writes
     , logical_reads 
from sys.dm_exec_sessions 
where program_name = 'SQLCMD'




*/

