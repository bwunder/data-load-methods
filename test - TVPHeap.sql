
:SETVAR LOADMETHOD "TVPHeap"                                        
:SETVAR RUNNOW "True"                                               
:SETVAR STAGINGDB "staging"                                         
:SETVAR HINT ""--"WITH(TABLOCK)"                                        
--------------------------------------------------------------------
-- send a table value parameter chocked full of rows (2+)
---------------------------------------------------------------------

SET NOCOUNT ON;
GO

use $(STAGINGDB);
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

SET @StartDt = SYSDATETIME(); 
SET @LoadMethod = '$(LOADMETHOD)';  

-- logging
SET @MessageCount = 0;
SET @LoadedRows = 0;
SET @Load_ms = 0;

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

    IF @LoggingLevel > 0
      SET @TimerDT = SYSDATETIME();
 
    -- implicit transaction
    INSERT dbo.vSourceDataWriter $(HINT)
      ( RowNumber 
      , Host
      , GMTDateTime 
      , Name 
      , Value 
      , FloatingPoint
      , Boolean 
      , Bytes 
      , LOB 
      , LOBCheckSum )
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
     FROM @TableVariable; 

    SET @LoadedRows += @@ROWCOUNT;
    IF @LoggingLevel > 0
      BEGIN
        SET @Load_ms += DATEDIFF(ms, @TimerDT, SYSDATETIME());
      END;

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
       + CASE WHEN '$(HINT)' = 'WITH(TABLOCK)'
              THEN ''
              ELSE 'NOTABLOCK'
             END            
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
    DECLARE @TruncateTable NVARCHAR(500);
    SET @TruncateTable = (SELECT 'TRUNCATE TABLE [' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']' 
                          FROM INFORMATION_SCHEMA.VIEW_TABLE_USAGE 
                          WHERE VIEW_NAME = 'vSourceDataWriter');  
    EXEC sp_executesql @TruncateTable;

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
  
  END; 

GO 

USE master;
