/*
select * from target.dbo.tActivityHistory Where loadmethod like 'FileBulkInsertHeap%'
--delete target.dbo.tActivityHistory Where loadmethod like 'FileBulkInsertHeap%'
is there a performance difference between -b and the ROWS_PER_BATCH hint BOL says something vague about a transaction
*/

-- must include trailing "\" in file system paths - not checked    
:SETVAR LOADMETHOD "FileBulkInsertHeap"                                                                   

:SETVAR SKIPTRUNCATE ""   -- "" or "--" comments out the truncate of target heap between each load    
:SETVAR BCPDATATYPES "N"              -- n, c, w, N need to match DATAFILETYPES                       
:SETVAR DATAFILETYPE "widenative"     -- native, char, widechar, widenative no space or hyphen!       
:SETVAR LOADHINT ""       -- ", TABLOCK" or ""                                            
 
:SETVAR BATCHSIZE 1000    -- same as ROWS_PER_BATCH

:SETVAR INITIATORDB "initiator"      -- must match db name in 1 - b - CREATE staging.sql              
:SETVAR INITIATORSQL "BILL764\ATER2"                                                                  
:SETVAR TARGETDB "target"            -- must match db name in 1 - a - CREATE target.sql               
:SETVAR STAGINGDB "staging"          -- must match db name in 1 - b - CREATE staging.sql              
:SETVAR SOURCEDATAPATH "C:\Users\bwunder\Documents\SQL Server Management Studio\Projects\data load methods\data load methods\bcpout\" 
:SETVAR BCPPATH "C:\Program Files\Microsoft SQL Server\100\Tools\Binn\" -- blank OK if no sideBySide? 
----------------------------------------------------------------------------------------------------
-- BULK INSERT a data file into SQL Server (runs in SQL Server process)
----------------------------------------------------------------------------------------------------
SET NOCOUNT ON;
GO

use $(STAGINGDB);

GO

IF ( SELECT ISNULL(MAX(index_id), -1) FROM sys.indexes 
     WHERE object_id = OBJECT_ID('dbo.StagedData') ) <> 0 
  BEGIN
    IF EXISTS (SELECT * FROM sys.indexes 
               WHERE name = 'ixn_StagedData__Name__LOBSize__GMTDateTime'
               AND object_id = OBJECT_ID('dbo.StagedData'))
      DROP INDEX dbo.StagedData.ixn_StagedData__Name__LOBSize__GMTDateTime      
    ELSE   
      RAISERROR ('$(STAGINGDB).dbo.StagedData is not a heap or is not found. Test Aborted.',20,1) WITH LOG; 
  END

GO

IF CONVERT(VARBINARY(2), N'$(BCPDATATYPES)') NOT IN (0x4E, 0x63, 0x6E, 0x77) -- N, c, n, w
 RAISERROR('Invalid BCP Datatypes specification "$(BCPDATATYPES)". Must be "N", "c", "n" or "w" (case sensitive).'
          , 20, 1) WITH LOG


GO

IF OBJECT_ID('tempdb.dbo.#$(LOADMETHOD)_Log','U') IS NOT NULL
  DROP TABLE #$(LOADMETHOD)_Log;

CREATE TABLE #$(LOADMETHOD)_Log
  ( StartDT DATETIME2 );

GO

IF OBJECT_ID('dbo.$(LOADMETHOD)', 'P') IS NOT NULL
  DROP PROCEDURE dbo.$(LOADMETHOD)

GO

CREATE PROCEDURE dbo.$(LOADMETHOD)
AS
BEGIN

  DECLARE @LoadedRows BIGINT
        , @Load_ms BIGINT  
        , @LoggingLevel TINYINT
        , @RowCount INT
        , @RowsRequested INT
        , @BatchSize INT
        , @TimerDT DATETIME2
        , @SQLStr NVARCHAR(1000);

  SET @LoggingLevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);
  SET @RowsRequested = (SELECT RowsRequested FROM dbo.tTransferParameters);
  SET @BatchSize = $(BATCHSIZE)

  -- could skip truncate if loading to heap with existing data fits scenario
  $(SKIPTRUNCATE)TRUNCATE TABLE dbo.StagedData;

  IF @LoggingLevel > 0
    SET @TimerDT = SYSDATETIME();
 
  SET @SQLStr = N'BULK INSERT $(STAGINGDB).dbo.StagedData '
              + N'FROM ''$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data'' '
              + N'WITH ( BATCHSIZE = ' + CAST(@Batchsize AS NVARCHAR(10)) 
                   + N', DATAFILETYPE = ''$(DATAFILETYPE)'' '
                   + N'$(LOADHINT)'
                   + N', ROWS_PER_BATCH  = ' + CAST(@RowsRequested AS NVARCHAR(10))
                   + N', ORDER ([RowNumber]));';
              
  EXEC sp_executesql @SQLStr;             

  SET @LoadedRows = @@ROWCOUNT;

  IF @LoggingLevel > 0
    BEGIN
      -- do not include the log write in the time
      SET @Load_ms = DATEDIFF(ms, @TimerDT, SYSDATETIME());

      INSERT target.dbo.tActivityHistory
        ( LoadMethod	
        , StartDT
        , LoadedRows
        , Load_ms
        , Elapsed_ms
        , MessageQuantity
        , RowsToPush
        , ReceiveMsgWaitTime 
        , NumberInitiatorTransactions
        , SendTxnWaitTime
        , NumberDialogs
        , DialogRecycleMaxMessages
        , MaxQueueReaders
        , MaxMessagesPerReceive
        , LoggingLevel
        , RowVersioning
        , DataCompression
        , FillFactorPct
        , DateIndex
        , IdIndex )
      SELECT '$(LOADMETHOD)_$(BCPDATATYPES)_' + CASE WHEN '$(LOADHINT)' = ''
                                                     THEN 'NOTABLOCK'
                                                     ELSE ''
                                                     END
        , l.StartDT 
        , @LoadedRows
        , @Load_ms
        , DATEDIFF(ms, l.StartDT, SYSDATETIME()) 
        , p.MessageQuantity
        , p.RowsToPush
        , p.ReceiveMsgWaitTime 
        , p.NumberInitiatorTransactions
        , p.SendTxnWaitTime
        , p.NumberDialogs
        , p.DialogRecycleMaxMessages
        , p.MaxQueueReaders
        , p.MaxMessagesPerReceive
        , p.LoggingLevel
        , p.RowVersioning
        , p.DataCompression
        , p.FillFactorPct
        , p.DateColumnIndexType
        , p.IdColumnIndexType
      FROM #$(LOADMETHOD)_LOG l
      CROSS APPLY target.dbo.tTransferParameters p;

    END

END

GO

-- same load sequence as other scenarios
:SETVAR ROWS 10000
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 1
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 5
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 10
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  

GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 50                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 100                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 500                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 1000                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO


:SETVAR ROWS 5000                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 10000                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 50000                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 100000                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 10000                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 5000                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 1000                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 500                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 100                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 500                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 100                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 50                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 10                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 5                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
EXEC $(LOADMETHOD); 
GO

:SETVAR ROWS 1                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data" -v -$(BCPDATATYPES) -T -S$(INITIATORSQL)
GO
EXEC $(LOADMETHOD); 
GO
UPDATE #$(LOADMETHOD)_Log  SET StartDT = SYSDATETIME();  
GO
EXEC $(LOADMETHOD); 
GO

USE master;
GO 