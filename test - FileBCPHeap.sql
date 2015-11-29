/*
select * from target.dbo.tActivityHistory Where loadmethod like 'FileBCPHeap%N'
--delete target.dbo.tActivityHistory Where loadmethod like 'FileBCPHeapNOTABLOCK_N%'
is there a performance difference between -b and the ROWS_PER_BATCH hint BOL says something vague about a transaction
*/

-- reset the connecion if you change the LOADMETHOD value
-- must include trailing "\" in file system paths - not checked    
:SETVAR LOADMETHOD "FileBCPHeap"          -- DATATYPE is suffixed to LOADMETHOD in the script   

:SETVAR DATATYPES "N"                          -- n=native, c=char, w=widechar, N=widenative         
:SETVAR LOADHINT "-h TABLOCK"                 -- "-h TABLOCK" or ""                                  
:SETVAR SKIPTRUNCATE ""    -- "--" or "" comments out the truncate of target table between each load 

:SETVAR INITIATORDB "initiator"                                                                      
:SETVAR STAGINGDB "staging"                                                                          
:SETVAR TARGETDB "target"                                                                            
:SETVAR SOURCEDATAPATH "C:\Users\bwunder\Documents\SQL Server Management Studio\Projects\data load methods\data load methods\bcpout\" 
:SETVAR INITIATORSQL "BILL764\ATER2"                                                                 
:SETVAR TARGETSQL "BILL764\ATER2"                                                                    
:SETVAR BCPPATH "C:\Program Files\Microsoft SQL Server\100\Tools\Binn\" -- blank OK if no sideBySide 
SET NOCOUNT ON
GO

USE $(STAGINGDB);

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

IF OBJECT_ID('tempdb.dbo.#$(LOADMETHOD)_Log','U') IS NOT NULL
  DROP TABLE #$(LOADMETHOD)_Log;

CREATE TABLE #$(LOADMETHOD)_Log
  ( StartDT DATETIME2
  , LoadStartDT DATETIME2 );

GO

IF OBJECT_ID('tempdb.dbo.#$(LOADMETHOD)_LogActivityHistory','P') IS NOT NULL
  DROP PROCEDURE #$(LOADMETHOD)_LogActivityHistory;

GO

CREATE PROCEDURE #$(LOADMETHOD)_LogActivityHistory
AS
BEGIN


  INSERT dbo.tActivityHistory
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
  SELECT '$(LOADMETHOD)_$(DATATYPES)' + CASE WHEN '$(LOADHINT)' = '-h TABLOCK'
                                             THEN ''
                                             ELSE 'NOTABLOCK'
                                             END
    , l.StartDT 
    , s.Rows
    , DATEDIFF(ms, l.LoadStartDT, SYSDATETIME())  
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
  FROM #$(LOADMETHOD)_Log l
  CROSS JOIN (SELECT part.rows
              FROM sys.objects AS obj
              JOIN sys.partitions AS part 
              ON part.object_id = obj.object_id
              WHERE obj.name = 'StagedData') s
  CROSS JOIN dbo.tTransferParameters p;

  TRUNCATE TABLE #$(LOADMETHOD)_Log;

  -- not required - makes it easier to get the loadedRows
  $(SKIPTRUNCATE)TRUNCATE TABLE dbo.StagedData;

END;

GO

-- same load sequence as other scenarios
:SETVAR ROWS 10000
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS) -a 65535
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 1
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 5
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 10
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 50                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 100                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 500                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 1000                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO


:SETVAR ROWS 5000                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 10000                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 50000                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 100000                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 10000                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 5000                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 1000                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 500                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 100                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 500                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 100                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 50                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 10                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 5                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

:SETVAR ROWS 1                            
GO
UPDATE dbo.tTransferParameters SET RowsToPush = $(ROWS); 
INSERT #$(LOADMETHOD)_Log (StartDT) Values (SYSDATETIME());  
GO
!!cd $(BCPPATH)&bcp.exe "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(LOADMETHOD).data" -v -$(DATATYPES) -T -S$(INITIATORSQL)
GO
UPDATE #$(LOADMETHOD)_Log SET LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) $(LOADHINT) -b $(ROWS)
GO
EXEC #$(LOADMETHOD)_LogActivityHistory; 
GO

USE master;
GO 