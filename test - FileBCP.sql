-- must include trailing "\" in file system paths - not checked    
:SETVAR LOADMETHOD "FileBCP"                  -- DATATYPES is suffixed to LOADMETHOD in the script   

:SETVAR LOADHINT ""                  -- ",TABLOCK" or ""                                     
:SETVAR PACKETSIZE "65535"                     -- 4096 is default
:SETVAR BATCHSIZE 1000                       -- BCP batch size default is 1000                       

:SETVAR DATATYPES "N"                          -- n=native, c=char, w=widechar, N=widenative         

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

IF CONVERT(VARBINARY(2), N'$(DATATYPES)') NOT IN (0x4E, 0x63, 0x6E, 0x77) -- N, c, n, w
 RAISERROR('Invalid BCP datatypes specification "%s". Must be "N", "c", "n" or "w" (case sensitive).'
          , 20, 1, N'$(DATATYPES)') WITH LOG

GO

IF OBJECT_ID('tempdb.dbo.#$(LOADMETHOD)_Log','U') IS NOT NULL
  DROP TABLE #$(LOADMETHOD)_Log;

GO

CREATE TABLE #$(LOADMETHOD)_Log
  ( StartDT DATETIME2
  , LoadStartDT DATETIME2 );

GO

IF OBJECT_ID('dbo.$(LOADMETHOD)','P') IS NOT NULL
  DROP PROCEDURE dbo.$(LOADMETHOD);

GO

CREATE PROCEDURE dbo.$(LOADMETHOD)
AS
BEGIN

DECLARE @LoggingLevel TINYINT
      , @LoadMethod NVARCHAR(128)
      , @CreateIndex_ms BIGINT
      , @MergeParentRows BIGINT
      , @MergeParent_ms BIGINT
      , @IndexMapper_ms BIGINT
      , @InsertChildRows BIGINT
      , @InsertChild_ms BIGINT
      , @RowCount INT
      , @BCPRows BIGINT
      , @BCP_ms BIGINT
      , @UpsertedRows BIGINT
      , @Upsert_ms BIGINT
      , @LoadedRows BIGINT
      , @Load_ms BIGINT
      , @TimerDT DATETIME2;

  SET @LoadMethod = '$(LOADMETHOD)';
  SET @LoggingLevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);

  IF @LoggingLevel > 1
    BEGIN
      SET @BCPRows = (SELECT COUNT(*) FROM dbo.StagedDate);
      SET @BCP_ms = (SELECT DATEDIFF(ms, LoadStartDt, SYSDATETIME()) FROM #$(LOADMETHOD)_Log);
    END

  IF @LoggingLevel > 1
    SET @TimerDT = SYSDATETIME();

  EXEC dbo.TargetUpsert @LoggingLevel
                      , @LoadMethod
                      , @CreateIndex_ms OUTPUT
                      , @MergeParentRows OUTPUT
                      , @MergeParent_ms OUTPUT
                      , @IndexMapper_ms OUTPUT
                      , @InsertChildRows OUTPUT
                      , @InsertChild_ms OUTPUT;
   
  IF @LoggingLevel > 1
    SELECT DATEDIFF(ms, @TimerDT, SYSDATETIME()) AS [Upsert_ms]
         , @InsertChildRows AS [UpsertedRows];

  --LogActivityHistory
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
  SELECT '$(LOADMETHOD)_$(DATATYPES)_' + CASE WHEN '$(LOADHINT)' = ''
                                              THEN 'NOTABLOCK'
                                              ELSE ''
                                              END
    , l.StartDT 
    , @InsertChildRows
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
  CROSS JOIN dbo.tTransferParameters p;

  TRUNCATE TABLE #$(LOADMETHOD)_Log;
  TRUNCATE TABLE dbo.StagedData;

END

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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD); 
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD); 
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
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
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
GO
UPDATE #$(LOADMETHOD)_Log SET StartDT = NULL, LoadStartDT = SYSDATETIME();
GO
!!cd $(BCPPATH)&bcp.exe staging.dbo.StagedData in "$(SOURCEDATAPATH)$(LOADMETHOD).data" -$(DATATYPES) -T -S$(TARGETSQL) -h ROWS_PER_BATCH=$(ROWS)$(LOADHINT) -a $(PACKETSIZE)
GO
EXEC dbo.$(LOADMETHOD);
GO

USE master;
GO 