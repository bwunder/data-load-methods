-- must include trailing "\" in file system paths - not checked, just make sure to add it    
:SETVAR LOADMETHOD "FileBulkInsert"                                                               

:SETVAR BCPDATATYPES "N"              -- n, c, w, N need to match DATAFILETYPES                       
:SETVAR DATAFILETYPE "widenative"     -- native, char, widechar, widenative no space or hyphen!       
:SETVAR LOADHINT ""          -- ", TABLOCK" or ""                                            
 
:SETVAR INITIATORDB "initiator"      -- must match db name in 1 - b - CREATE staging.sql              
:SETVAR INITIATORSQL "BILL764\ATER2"                                                                  
:SETVAR TARGETDB "target"            -- must match db name in 1 - a - CREATE target.sql               
:SETVAR STAGINGDB "staging"          -- must match db name in 1 - b - CREATE staging.sql              
:SETVAR SOURCEDATAPATH "C:\Users\bwunder\Documents\SQL Server Management Studio\Projects\data load methods\data load methods\bcpout\" 
:SETVAR BCPPATH "C:\Program Files\Microsoft SQL Server\100\Tools\Binn\" -- blank OK if no sideBySide? 
----------------------------------------------------------------------------------------------------
-- BULK INSERT a data file into SQL Server (runs in SQL Server process)
----------------------------------------------------------------------------------------------------
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

  DECLARE @Load_ms BIGINT  
        , @LoggingLevel TINYINT
        , @TimerDT DATETIME2
        , @DropIndex_ms BIGINT
        , @BulkInsertRows BIGINT
        , @BulkInsert_ms BIGINT
        , @CreateIndex_ms BIGINT
        , @MergeParentRows BIGINT
        , @MergeParent_ms BIGINT
        , @IndexMapper_ms BIGINT
        , @InsertChildRows BIGINT
        , @InsertChild_ms BIGINT;

  SET @LoggingLevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);

  TRUNCATE TABLE dbo.StagedData;

  IF @LoggingLevel > 0
    SET @TimerDT = SYSDATETIME();
 
  EXEC dbo.BulkInsertUpsert '$(SOURCEDATAPATH)$(LOADMETHOD)_$(DATAFILETYPE).data'
                          , '$(DATAFILETYPE)'
                          , @LoggingLevel
                          , '$(LOADMETHOD)'
                          , @DropIndex_ms OUTPUT
                          , @BulkInsertRows OUTPUT
                          , @BulkInsert_ms OUTPUT
                          , @CreateIndex_ms OUTPUT
                          , @MergeParentRows OUTPUT
                          , @MergeParent_ms OUTPUT
                          , @IndexMapper_ms OUTPUT
                          , @InsertChildRows OUTPUT
                          , @InsertChild_ms OUTPUT;
  IF @LoggingLevel > 1
    SELECT OBJECT_NAME(@@PROCID) AS LoadMethod
         , @DropIndex_ms AS [@DropIndex_ms]
         , @BulkInsertRows AS [@BulkInsertRows]
         , @BulkInsert_ms AS [@BulkInsert_ms]
         , @CreateIndex_ms AS [@CreateIndex_ms]
         , @MergeParentRows AS [@MergeParentRows]
         , @MergeParent_ms AS [@MergeParent_ms]
         , @IndexMapper_ms AS [@IndexMapper_ms]
         , @InsertChildRows AS [@InsertChildRows]
         , @InsertChild_ms AS [@InsertChild_ms]

  IF @LoggingLevel > 0
    BEGIN
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
      SELECT '$(LOADMETHOD)' + CASE WHEN '$(LOADHINT)' = ', TABLOCK' 
                               THEN '' 
                               ELSE 'NOTABLOCK' END
           , l.StartDT 
           , @InsertChildRows
           , DATEDIFF(ms, @TimerDT, SYSDATETIME())
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
:SETVAR ROWS 1000
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
