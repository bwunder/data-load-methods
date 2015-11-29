
:SETVAR LOADMETHOD "StageFileBCP"                -- DATATYPES is suffixedin the script              
:SETVAR DATATYPES "N"                            -- n=native, c=char, w=widechar, N=widenative      
:SETVAR INITIATORDB "initiator"                                                                     
:SETVAR STAGINGDB "staging"                                                                         
:SETVAR TARGETDB "target"                                                                           
:SETVAR ROWS 100000                          -- 10, 100, 500, 1,000, 5,000, 10,000, 50,000, 100,000 
:SETVAR SOURCEDATAPATH "C:\Users\bwunder\Desktop\data load methods\bcp\" --must include last "\"    
:SETVAR FILENAME "source.data"                                                                      
:SETVAR INITIATORSQL "BILL764\ATER2"                                                                
:SETVAR TARGETSQL "BILL764\ATER2"                                                                   
----------------------------------------------------------------------------------------------------
-- create then load a data file using BCP.exe (stand-alone executable)
----------------------------------------------------------------------------------------------------

IF CONVERT(VARBINARY(2), N'$(DATATYPES)') NOT IN (0x4E, 0x63, 0x6E, 0x77) -- N, c, n, w
 RAISERROR('Invalid BCP datatypes specification "%s". Must be "N", "c", "n" or "w" (case sensitive).'
          , 20, 1, N'$(DATATYPES)') WITH LOG

GO
USE $(STAGINGDB);

GO

SET NOCOUNT ON
GO
EXEC $(TARGETDB).dbo.pGetIndexDetails 'SAMPLED';

GO

DECLARE @LoggingLevel TINYINT;
DECLARE @LoadMethod NVARCHAR(128);

IF OBJECT_ID('tempdb.dbo.#Log','U') IS NOT NULL
  DROP TABLE #Log;

  CREATE TABLE #Log
    ( GenerateStartDT DATETIME2
    , GeneratedRows BIGINT
    , Generate_ms BIGINT
    , FetchStartDt DATETIME2
    , FetchedRows BIGINT
    , Fetch_ms BIGINT
    , LoadStartDt DATETIME2
    , LoadedRows BIGINT
    , Load_ms BIGINT 
    , DropIndex_ms BIGINT);

SET @LoadMethod = '$(LOADMETHOD)_$(DATATYPES)';
SET @LoggingLevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);

IF @LoggingLevel > 0
  INSERT #Log (GenerateStartDT)
  Values (SYSDATETIME());


-- NEED THe GOs around the BCPs TO FOR GOOD TIMES
GO
-----------------------------------------------------
-- BCP/BulkLoad methods 
----------------------------------------------------
!!bcp "SELECT * FROM $(INITIATORDB).dbo.fGetTableVariable(1,$(ROWS))" queryout "$(SOURCEDATAPATH)$(FILENAME)" -v -$(DATATYPES) -T -S$(INITIATORSQL)

GO

/* for example only YMMV:
relative sizes for 1000 rows, unicode data is lost if char mode is used
08/10/2010  02:15 PM           635,261 source_1000.char
08/10/2010  02:14 PM         1,161,272 source_1000.native
08/10/2010  02:14 PM         1,270,506 source_1000.widechar
08/10/2010  02:13 PM         1,183,888 source_1000.widenative
*/

!!DIR "$(SOURCEDATAPATH)$(FILENAME)"

GO

USE $(STAGINGDB);
GO
DECLARE @LoggingLevel TINYINT;
DECLARE @LoadMethod NVARCHAR(128);
DECLARE @RowCount INT;

IF(SELECT LoggingLevel FROM dbo.tTransferParameters) > 0
  UPDATE #Log
  SET Generate_ms = DATEDIFF(ms, GenerateStartDT, SYSDATETIME());


SET @RowCount = (SELECT COUNT(*) 
                 FROM dbo.StagedData);

SET @LoggingLevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);

IF @LoggingLevel > 0
  UPDATE #Log
  SET LoadStartDT = SYSDATETIME(); 

-- get the table ready
IF EXISTS (SELECT * FROM sys.indexes 
           WHERE name = 'ixn_StagedData__Name__LOBSize__GMTDateTime'
           AND object_id = OBJECT_ID('dbo.StagedData'))
 DROP INDEX [ixn_StagedData__Name__LOBSize__GMTDateTime] ON dbo.StagedData;
TRUNCATE TABLE dbo.StagedData;

IF @LoggingLevel > 1
  UPDATE #Log
  SET DropIndex_ms = DATEDIFF(ms, LoadStartDT, SYSDATETIME());
  
GO

!!bcp staging.dbo.StagedData in "$(SOURCEDATAPATH)$(FILENAME)" -$(DATATYPES) -T -S$(TARGETSQL)

GO

USE $(STAGINGDB);
GO
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
      , @TimerDT DATETIME2;


SET @LoadMethod = '$(LOADMETHOD)_$(DATATYPES)';
SET @LoggingLevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);

IF @LoggingLevel > 1
  BEGIN
    SET @BCPRows = (SELECT COUNT(*) FROM dbo.StagedDate);
    SET @BCP_ms = (SELECT DATEDIFF(ms, LoadStartDt, SYSDATETIME()) FROM #Log);
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
  UPDATE #Log
  SET @Upsert_ms = DATEDIFF(ms, @TimerDT, SYSDATETIME())
    , @UpsertedRows = @InsertChildRows;

IF @LoggingLevel > 0
  UPDATE #Log
  SET Load_ms = DATEDIFF(ms, LoadStartDT, SYSDATETIME())
    , LoadedRows =  @InsertChildRows;
  
IF @LoggingLevel > 0
  INSERT dbo.tActivityHistory
    ( LoadMethod	
    , StartDT
    , GeneratedRows
    , Generate_ms
    , FetchedRows
    , Fetch_ms
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
  SELECT '$(LOADMETHOD)_$(DATATYPES)' 
    , l.GenerateStartDT 
    , NULL
    , l.Generate_ms
    , NULL
    , NULL
    , l.LoadedRows
    , l.Load_ms
    , DATEDIFF(ms, l.GenerateStartDT, SYSDATETIME()) 
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
  FROM #Log l
  CROSS JOIN dbo.tTransferParameters p;

IF @LoggingLevel > 1
 SELECT @BCPRows AS [@BCPRows]
      , @BCP_ms AS [@BCP_ms]
      , DropIndex_ms AS [DropIndex_ms]
      , @UpsertedRows AS [@UpsertedRows]
      , @Upsert_ms AS [@Upsert_ms]                    
      , @CreateIndex_ms AS [@CreateIndex_ms]
      , @MergeParentRows AS [@MergeParentRows] 
      , @MergeParent_ms AS [@MergeParent_ms]
      , @IndexMapper_ms AS [@IndexMapper_ms]
      , @InsertChildRows AS [@InsertChildRows]
      , @InsertChild_ms AS [@InsertChild_ms]
  FROM #Log; 

-- tiny chance it will be the wrong row
EXEC dbo.pGetActivityForLoadMethod '$(LOADMETHOD)_$(DATATYPES)'
EXEC dbo.pGetIndexDetails 'SAMPLED'

GO
USE master;
GO 