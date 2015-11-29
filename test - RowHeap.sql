
:SETVAR LOADMETHOD "RowHeap"                                                 
:SETVAR RUNNOW "True"            -- "True" "False"                           
:SETVAR STAGINGDB "staging"                                                  
:SETVAR TABLOCK "true"           -- "true" "false"                           
:SETVAR NOXACT ""                -- "" "--" to comments out xact statements  
--:SETVAR RECOVERY "SIMPLE"   -- "BULK_LOGGED"  "SIMPLE"             
-------------------------------------------------------------------------
-- load a table value parameter of rows into an unindexed table
-------------------------------------------------------------------------
SET NOCOUNT ON;
GO

USE [master]
GO

--ALTER DATABASE [$(STAGINGDB)] SET RECOVERY $(RECOVERY) WITH NO_WAIT
--GO

use $(STAGINGDB);
GO

IF OBJECT_ID('dbo.$(LOADMETHOD)', 'P') IS NOT NULL
  DROP PROCEDURE dbo.$(LOADMETHOD);

GO

CREATE PROCEDURE dbo.$(LOADMETHOD)
AS
BEGIN

  DECLARE @MessageCount INT
        , @MessageQuantity INT
        , @StartDT DATETIME2
        , @NextRowNumber BIGINT 
        , @RowsToPush INT 
        , @LoggingLevel TINYINT
        , @LoadMethod NVARCHAR(128)
        , @RowCount INT
        , @TimerDT DATETIME2
        , @LoadedRows BIGINT
        , @Load_ms BIGINT
        , @junk INT;
   
  SET @LoadMethod = '$(LOADMETHOD)';  
  SET @StartDt = SYSDATETIME(); 

  -- logging
  SET @LoadedRows = 0;
  SET @Load_ms = 0;

  SET @LoggingLevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);
  SET @MessageQuantity = (SELECT MessageQuantity FROM dbo.tTransferParameters)
  SET @RowsToPush = (SELECT RowsToPush FROM dbo.tTransferParameters);

  SET @MessageCount = 0;
  WHILE @MessageCount < @MessageQuantity
    BEGIN
          
      BEGIN TRY
      
        IF @LoggingLevel > 0
          SET @TimerDT = SYSDATETIME();
        SET @RowCount = 1; 

        $(NOXACT)BEGIN TRANSACTION LoadRowsToHeap;
   
          -- lock the table once per batch 
          IF '$(TABLOCK)' = 'true'
            SET @junk = (SELECT TOP(1) RowNumber 
                         FROM dbo.vSourceDataWriter WITH(TABLOCK,HOLDLOCK) );
 
 
          WHILE @RowCount <= @RowsToPush
            BEGIN
              -- locking here degrades perf at low rowsizes 
              INSERT dbo.vSourceDataWriter
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
              FROM dbo.fGetTableVariable(@RowCount + @MessageCount * @RowsToPush, 1);
              
              IF @@ROWCOUNT = 0 BREAK;

              SET @RowCount += 1;  
              SET @LoadedRows += 1;

            END;

        $(NOXACT)COMMIT TRANSACTION LoadRowsToHeap;

        IF @LoggingLevel > 0
          SET @Load_ms += DATEDIFF(ms, @TimerDT, SYSDATETIME());

        SET @MessageCount += 1; 

      END TRY
    
      BEGIN CATCH

        SELECT 
            ERROR_NUMBER() AS ErrorNumber,
            ERROR_SEVERITY() AS ErrorSeverity,
            ERROR_STATE() as ErrorState,
            ERROR_PROCEDURE() as ErrorProcedure,
            ERROR_LINE() as ErrorLine,
            ERROR_MESSAGE() as ErrorMessage;

        $(NOXACT)IF XACT_STATE() <> 0
        $(NOXACT)  ROLLBACK TRANSACTION LoadRowsToHeap;

      END CATCH;
   
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
         + CASE WHEN '$(TABLOCK)' = 'true'
                THEN ''
                ELSE CASE WHEN '$(NOXACT)' = ''
                          THEN 'NOTABLOCK'
                          ELSE 'NOXACT'
                          END
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

    IF '$(TABLOCK)' = 'true'
    AND '$(NOXACT)' = ''
      EXEC dbo.pGetActivityForLoadMethod '$(LOADMETHOD)';
    ELSE
      EXEC dbo.pGetActivityForLoadMethod '$(LOADMETHOD)NOTABLOCK';
  
  END; 

GO 

USE master;
