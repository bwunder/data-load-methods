/*
UPDATE dbo.tTransferParameters SET RowsToPush = 100000
select * from dbo.tTransferParameters
*/
:SETVAR TARGETDB "target"
:SETVAR STAGINGDB "staging"
:SETVAR LOADMETHOD "StageABTables_X3"
:SETVAR RUNNOW "False" -- True False      

----------------------------------------------
-- simple, persistent and reliable
-- send one set of rows at a time, or could
-- be continuous/intermittent data coming in
-- then periodically load. Would be apparantly 
-- live to both the source and target
-- Only subsecond pause in collection required 
-- when the accumulated rows are loaded to the 
-- hierarchy. If the source ap handles the flop 
-- over, coordination is straight forward, if not
-- a mutex method is needed.  
----------------------------------------------

SET NOCOUNT ON;
GO

USE $(STAGINGDB);

GO
IF OBJECT_ID('dbo.$(LOADMETHOD)', 'P') IS NOT NULL
  DROP PROCEDURE dbo.$(LOADMETHOD)

GO

CREATE PROCEDURE dbo.$(LOADMETHOD)
AS
BEGIN
DECLARE @MessageCount INT
      , @MessageQuantity INT
      , @RowsToPush INT
      , @LoggingLevel TINYINT
      , @LoadMethod NVARCHAR(128)
      , @RowCount INT
      , @GeneratedRows BIGINT
      , @Generate_ms BIGINT
      , @LoadedRows BIGINT
      , @Load_ms BIGINT
      , @SwapViews_ms BIGINT
      , @MergeParentRows BIGINT
      , @MergeParent_ms BIGINT
      , @IndexMapper_ms BIGINT
      , @InsertChildRows BIGINT
      , @InsertChild_ms BIGINT
      , @StartDT DATETIME2
      , @TimerDT DATETIME2;

SET @LoadMethod = '$(LOADMETHOD)';
SET @StartDT = SYSDATETIME();

SET @MessageQuantity = (SELECT MessageQuantity FROM dbo.tTransferParameters);    
SET @RowsToPush = (SELECT RowsToPush FROM dbo.tTransferParameters);    
SET @LoggingLevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);

SET @GeneratedRows = 0;
SET @Generate_ms = 0;
SET @LoadedRows = 0;
SET @Load_ms = 0;

SET @MessageCount = 0;
WHILE @MessageCount < @MessageQuantity
 BEGIN
   
  IF @LoggingLevel > 0
    SET @TimerDT = SYSDATETIME();

      BEGIN TRY
      
          SET @TimerDT = SYSDATETIME();

          $(NOXACT)BEGIN TRANSACTION StageRows;

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
           FROM dbo.fGetTableVariable(@MessageCount * @RowsToPush, @RowsToPush);
   
           SET @LoadedRows = @@ROWCOUNT;  

           $(NOXACT)COMMIT TRANSACTION LoadRowsToHeap;

           SET @Load_ms += DATEDIFF(ms, @TimerDT, SYSDATETIME());

      END TRY
    
      BEGIN CATCH

        SELECT 
            ERROR_NUMBER() AS ErrorNumber,
            ERROR_SEVERITY() AS ErrorSeverity,
            ERROR_STATE() as ErrorState,
            ERROR_PROCEDURE() as ErrorProcedure,
            ERROR_LINE() as ErrorLine,
            ERROR_MESSAGE() as ErrorMessage;

        $(NOXACT)ROLLBACK TRANSACTION LoadRowsToHeap;

      END CATCH;

    SET @MessageCount += 1; 

 END;
   
IF @LoggingLevel > 0
  INSERT $(TARGETDB).dbo.tActivityHistory
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
  SELECT '$(LOADMETHOD)' 
    , @StartDT 
    , @GeneratedRows
    , @Generate_ms
    , NULL
    , NULL
    , NULL
    , @LoadedRows
    , @Load_ms
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
  FROM dbo.tTransferParameters p;

  RETURN CASE WHEN SCOPE_IDENTITY() = IDENT_CURRENT('dbo.tActivityHistory')
              THEN SCOPE_IDENTITY()
              ELSE 0
              END;

END;

GO

exec dbo.StageABTables_X3;