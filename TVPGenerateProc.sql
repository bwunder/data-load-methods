--STYLE TVP
--:SETVAR METHOD "Merge"   -- Merge  Upsert                                               
--:SETVAR TARGETDB "target"                                                                
--:SETVAR LOCKTYPE "ROW"           -- ROW  PAG  TAB                                        
--:SETVAR ISOTYPE "LCK"            -- VER  LCK                                             
--:SETVAR STAGINGDB "staging"
-----------------------------------------------------------------------------------------
--                  send a set of rows as a table valued parameter                     --
-----------------------------------------------------------------------------------------
SET NOCOUNT ON;

GO

USE $(TARGETDB);

GO

IF OBJECT_ID('dbo.TVP$(METHOD)$(ISOTYPE)$(LOCKTYPE)', 'P') IS NOT NULL
  DROP PROCEDURE dbo.TVP$(METHOD)$(ISOTYPE)$(LOCKTYPE)

GO

CREATE PROCEDURE dbo.TVP$(METHOD)$(ISOTYPE)$(LOCKTYPE)
  ( @RowsToPush INT, @ConcurrentCycles INT )
AS
BEGIN

  DECLARE @MessageCount INT
        , @StartDT DATETIME2
        , @TableVariable TVPTableType
        , @NextRowNumber INT 
        , @LoggingLevel TINYINT
        , @LoadMethod NVARCHAR(128)
        , @RowCount INT
        , @TimerDT DATETIME2
        , @LoadedRows BIGINT
        , @Load_ms BIGINT
        , @TargetLocker INT;

  SET NOCOUNT ON;

  SET @StartDt = SYSDATETIME(); 
  SET @LoadMethod = 'TVP$(METHOD)$(ISOTYPE)$(LOCKTYPE)';  

  -- logging
  SET @MessageCount = 0;
  SET @LoadedRows = 0;
  SET @Load_ms = 0;

  SET @NextRowNumber = 1;
  SET @LoggingLevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);

  IF '$(ISOTYPE)' = 'VER'
  AND (SELECT is_read_committed_snapshot_on 
       FROM sys.databases
       WHERE name = '$(TARGETDB)') = 0 
   BEGIN

    ALTER DATABASE $(TARGETDB)
    SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

    ALTER DATABASE $(TARGETDB)
    SET READ_COMMITTED_SNAPSHOT ON;

    ALTER DATABASE $(TARGETDB)
    SET MULTI_USER;

   END
  ELSE IF '$(ISOTYPE)' = 'LCK'
  AND (SELECT is_read_committed_snapshot_on 
       FROM sys.databases
       WHERE name = '$(TARGETDB)') = 1 
   BEGIN

    ALTER DATABASE $(TARGETDB)
    SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

    ALTER DATABASE $(TARGETDB)
    SET READ_COMMITTED_SNAPSHOT OFF;

    ALTER DATABASE $(TARGETDB)
    SET MULTI_USER;

   END

  WHILE @MessageCount < (SELECT MessageQuantity FROM dbo.tTransferParameters) 
   BEGIN
    SET @MessageCount += 1;

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
     FROM dbo.fGetTableVariable(@NextRowNumber, @RowsToPush);
     
     SET @NextRowNumber += @RowsToPush; 
       
     IF @LoggingLevel>0
       SET @TimerDT = SySDATETIME();

     BEGIN TRY 
     
       BEGIN TRANSACTION;
   
        IF '$(LOCKTYPE)' IN ('ROW','PAG','TAB')
          SET @TargetLocker = (SELECT TOP(1) p.Id
                               FROM dbo.tParent p WITH (XLOCK, $(LOCKTYPE)LOCK) 
                               JOIN dbo.tChild c WITh (XLOCK, $(LOCKTYPE)LOCK)
                               ON p.Id = c.ParentId);

         -- both pTVPMerge and pTVPUpsert are transactional
         EXEC @LoadedRows = dbo.pTVP$(METHOD) @TableVariable
                                             , @LoggingLevel
                                             , @LoadMethod;
               
       COMMIT TRANSACTION;  

     END TRY
     
     BEGIN CATCH

       DECLARE @Number INT
             , @Severity TINYINT
             , @State TINYINT
             , @Proc NVARCHAR(128)
             , @Line INT
             , @Message NVARCHAR(MAX);   

       SELECT @Number = ERROR_NUMBER()
           , @Severity = ISNULL(ERROR_SEVERITY(), 1)
            , @State = ISNULL(ERROR_STATE(), 1)
            , @Proc = ISNULL('db object' + SPACE(1) + ERROR_PROCEDURE() + SPACE(1), '')
            , @Line = ERROR_LINE()
            , @Message = N':' + CHAR(13) + CHAR(10) + ERROR_MESSAGE();

       IF XACT_STATE() <> 0 
         ROLLBACK TRANSACTION; 

       RAISERROR ('Msg %d, Level %d, State %d, %s Line %d %s'
                ,16 ,1 ,@Number, @Severity, @State, @Proc, @Line, @Message)

     END CATCH

     IF @LoggingLevel > 0
       SET @Load_ms += DATEDIFF(ms, @TimerDT, SYSDATETIME());

     -- this could take some time 
     DELETE @TableVariable;
   
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
  SELECT @LoadMethod + CAST(@ConcurrentCycles AS NVARCHAR(10))       
       , @StartDT 
       , @LoadedRows
       , @Load_ms
       , DATEDIFF(ms, @StartDT, SYSDATETIME()) 
       , MessageQuantity
       , RowsToPush -- not meaningful in concurrency scenarios
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

END;

GO
