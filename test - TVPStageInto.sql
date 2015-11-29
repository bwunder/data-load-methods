
:SETVAR LOADMETHOD "TVPStageInto"
:SETVAR RUNNOW "True" -- True False      
:SETVAR TARGETDB "target"        -- must match target name in 1 - a - CREATE target.sql 
--------------------------------------------------------------------
-- send a table value parameter chocked full of rows (2+)
---------------------------------------------------------------------

SET NOCOUNT ON;
GO

use $(TARGETDB);
GO


IF OBJECT_ID('dbo.$(LOADMETHOD)', 'P') IS NOT NULL
  DROP PROCEDURE dbo.$(LOADMETHOD);

GO

CREATE PROCEDURE dbo.$(LOADMETHOD)
AS
BEGIN

DECLARE @MessageCount INT
      , @FirstRow INT
      , @StartDT DATETIME2
      , @TableVariable TVPTableType
      , @NextRowNumber BIGINT 
      , @RowsToPush INT 
      , @LoggingLevel TINYINT
      , @LoadMethod NVARCHAR(128)
      , @RowCount INT
      , @TimerDT DATETIME2
      , @LoadedRows BIGINT
      , @Load_ms BIGINT
      , @TVPToTempTableRows BIGINT
      , @TVPToTempTable_ms BIGINT 
      , @CreateIndex_ms BIGINT
      , @MergeParentRows BIGINT
      , @MergeParent_ms BIGINT
      , @InsertChildRows BIGINT
      , @InsertChild_ms BIGINT
      , @SumCreateIndex_ms BIGINT
      , @SumTVPToTempTableRows BIGINT
      , @SumTVPToTempTable_ms BIGINT 
      , @SumMergeParentRows BIGINT
      , @SumMergeParent_ms BIGINT
      , @SumInsertChildRows BIGINT
      , @SumInsertChild_ms BIGINT;

SET @StartDt = SYSDATETIME(); 
SET @LoadMethod = '$(LOADMETHOD)';  

-- logging
SET @MessageCount = 0;
SET @LoadedRows = 0;
SET @Load_ms = 0;
SET @MergeParentRows = 0;
SET @MergeParent_ms = 0;
SET @InsertChildRows = 0;
SET @InsertChild_ms = 0;
SET @SumCreateIndex_ms = 0;
SET @SumMergeParentRows = 0;
SET @SumMergeParent_ms = 0;
SET @SumInsertChildRows = 0;
SET @SumInsertChild_ms = 0;

SET @NextRowNumber = 1;
SET @RowsToPush = (SELECT RowsToPush FROM dbo.tTransferParameters);
SET @LoggingLevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);

SET @FirstRow = 1;
SET @MessageCount = 0;
WHILE @MessageCount < (SELECT MessageQuantity 
                       FROM dbo.tTransferParameters)
  BEGIN
     
    DELETE @TableVariable; 
     
    INSERT @TableVariable 
      ( RowNumber 
      , Host
      , GMTDateTime 
      , Name 
      , Value 
      , FloatingPoint
      , Boolean 
      , Bytes 
      , LOB 
      , LOBCheckSum)
    SELECT RowNumber 
         , Host
         , GMTDateTime 
         , Name 
         , Value 
         , FloatingPoint
         , Boolean 
         , Bytes 
         , LOB 
         , LOBCheckSum
    FROM dbo.fGetTableVariable(@FirstRow, @RowsToPush);

    SET @FirstRow += @RowsToPush; 

    SET @TVPToTempTableRows = 0;
    SET @TVPToTempTable_ms = 0;
    SET @CreateIndex_ms = 0;
    SET @MergeParentRows = 0;
    SET @MergeParent_ms = 0;
    SET @InsertChildRows = 0;
    SET @InsertChild_ms = 0;

    IF @LoggingLevel > 0
      SET @TimerDT = SYSDATETIME();
 
    EXEC dbo.StageUpsert @TableVariable
                       , @LoggingLevel
                       , @LoadMethod      
                       , @TVPToTempTableRows OUTPUT 
                       , @TVPToTempTable_ms OUTPUT
                       , @CreateIndex_ms OUTPUT 
                       , @MergeParentRows OUTPUT 
                       , @MergeParent_ms OUTPUT
                       , @InsertChildRows OUTPUT 
                       , @InsertChild_ms OUTPUT;    

    IF @LoggingLevel > 0
      BEGIN
        SET @Load_ms += DATEDIFF(ms, @TimerDT, SYSDATETIME());
        SET @LoadedRows += @InsertChildRows;
      END;

    IF @LoggingLevel > 1
      BEGIN 
        SET @SumTVPToTempTableRows += @TVPToTempTableRows;
        SET @SumTVPToTempTable_ms += @TVPToTempTable_ms;
        SET @SumCreateIndex_ms += @CreateIndex_ms;
        SET @SumMergeParentRows += @MergeParentRows;
        SET @SumMergeParent_ms += @MergeParent_ms;
        SET @SumInsertChildRows += @InsertChildRows;
      END;
      SET @SumInsertChild_ms += @InsertChild_ms;

    SET @MessageCount += 1; 

  END;

IF @LoggingLevel > 0
  BEGIN
    WHILE @@TRANCOUNT > 0
      BEGIN
        SELECT 'oops - dangling transaction';
        ROLLBACK;
      END;
  END;  

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
SELECT @LoadMethod 
  , @StartDT 
  , @LoadedRows
  , @Load_ms
  , DATEDIFF(ms, @StartDT, SYSDATETIME()) 
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
  , DateColumnIndexType
  , IdColumnIndexType
FROM dbo.tTransferParameters;

RETURN CASE WHEN SCOPE_IDENTITY() = IDENT_CURRENT('dbo.tActivityHistory')
            THEN SCOPE_IDENTITY()
            ELSE 0
            END;

END;

GO

IF '$(RUNNOW)' = 'True'

  BEGIN

    EXEC dbo.pGetIndexDetails 'SAMPLED'

    UPDATE dbo.tTransferParameters SET RowsToPush = 10000;
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 1;
    EXEC dbo.$(LOADMETHOD);
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 5;
    EXEC dbo.$(LOADMETHOD);
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 10;
    EXEC dbo.$(LOADMETHOD);
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 50;
    EXEC dbo.$(LOADMETHOD);
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 100;
    EXEC dbo.$(LOADMETHOD);
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 500;
    EXEC dbo.$(LOADMETHOD);
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 1000;
    EXEC dbo.$(LOADMETHOD);
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 5000;
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 10000;
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 50000;
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 100000;
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 10000;
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 5000;
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 1000;
    EXEC dbo.$(LOADMETHOD);
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 500;
    EXEC dbo.$(LOADMETHOD);
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 100;
    EXEC dbo.$(LOADMETHOD);
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 50;
    EXEC dbo.$(LOADMETHOD);
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 10;
    EXEC dbo.$(LOADMETHOD);
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 5;
    EXEC dbo.$(LOADMETHOD);
    EXEC dbo.$(LOADMETHOD);
    UPDATE dbo.tTransferParameters SET RowsToPush = 1;
    EXEC dbo.$(LOADMETHOD);
    EXEC dbo.$(LOADMETHOD);

    EXEC dbo.pGetActivityForLoadMethod '$(LOADMETHOD)'
    EXEC dbo.pGetIndexDetails 'SAMPLED';

  END; 

GO 

USE master;
