--STYLE ROW
--:SETVAR METHOD "Merge"   -- Merge  Upsert                                               
--:SETVAR TARGETDB "target"                                                                
--:SETVAR ISOTYPE "LCK"            -- VER  LCK                                             
--:SETVAR LOCKTYPE "ROW"           -- ROW  PAG  TAB                                        
--:SETVAR CONCURRENTCOUNT 1
--:SETVAR STAGINGDB "staging"
----------------------------------------------------------------------------
-- insert one child entity at a time, parent may update or insert 
----------------------------------------------------------------------------

SET NOCOUNT ON;

GO

use $(TARGETDB);

GO

IF OBJECT_ID('dbo.Row$(METHOD)$(ISOTYPE)$(LOCKTYPE)', 'P') IS NOT NULL
  DROP PROCEDURE dbo.Row$(METHOD)$(ISOTYPE)$(LOCKTYPE);

GO

CREATE PROCEDURE dbo.Row$(METHOD)$(ISOTYPE)$(LOCKTYPE)
  ( @RowsToPush INT, @ConcurrentCycles INT )
AS
BEGIN

  DECLARE @RowNumber INT
        , @Host NVARCHAR(128)
        , @GMTDateTime DATETIME
        , @Name VARCHAR(50)
        , @Value INT 
        , @FloatingPoint REAL
        , @Boolean BIT
        , @Bytes VARBINARY(60)
        , @LOB NVARCHAR(MAX)
        , @LOBCheckSum INT
        , @TableVariable TVPTableType
        , @RowCount INT
        , @MessageCount INT
        , @MessageQuantity INT
        , @StartDT DATETIME2
        , @LoggingLevel TINYINT
        , @TimerDT DATETIME2
        , @LoadedRows BIGINT
        , @Load_ms BIGINT
        , @TargetLocker INT
        , @hr INT;

  SET NOCOUNT ON;

  SET @StartDt = SYSDATETIME(); 

  SET @LoggingLevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);
  SET @MessageQuantity = (SELECT MessageQuantity FROM dbo.tTransferParameters);

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

  SET @MessageCount = 0;
  SET @LoadedRows = 0;
  SET @Load_ms = 0;

  BEGIN TRY

    WHILE @MessageCount < @MessageQuantity
     
      BEGIN
          
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
        FROM dbo.fGetTableVariable((@MessageCount * @RowsToPush) + 1, @RowsToPush);

        BEGIN TRANSACTION;
        
          IF '$(LOCKTYPE)' IN ('ROW','PAG','TAB')
            SET @TargetLocker = (SELECT TOP(1) p.Id
                                 FROM dbo.tParent p WITH (XLOCK, $(LOCKTYPE)LOCK) 
                                 JOIN dbo.tChild c WITh (XLOCK, $(LOCKTYPE)LOCK)
                                 ON p.Id = c.ParentId);

          SET @RowCount = 1;
          WHILE @RowCount <= @RowsToPush
          
            BEGIN
                             
              IF @LoggingLevel > 0 
                SET @TimerDT = SYSDATETIME();

              SELECT @Rownumber = [RowNumber]
                   , @Host = [Host]
                   , @GMTDateTime = [GMTDateTime]
                   , @Name = [Name]
                   , @Value = [Value]
                   , @FloatingPoint = [FloatingPoint]
                   , @Boolean = [Boolean]
                   , @Bytes = [Bytes] 
                   , @LOB = [LOB]
                   , @LOBCheckSum = [LOBCheckSum]
              FROM @TableVariable 
              WHERE RowNumber = (@MessageCount * @RowsToPush) + @RowCount;

              IF @LoggingLevel > 0 
                SET @TimerDT = SYSDATETIME();

              EXEC @hr = dbo.p$(LOADMETHOD) @RowNumber 
                                          , @Host
                                          , @GMTDateTime 
                                          , @Name 
                                          , @Value 
                                          , @FloatingPoint
                                          , @Boolean 
                                          , @Bytes 
                                          , @LOB 
                                          , @LOBCheckSum
                                          , @LoggingLevel
                                          , $(LOADMETHOD) ;
          

              IF @LoggingLevel > 0
                BEGIN
                  SET @LoadedRows += @hr; 
                  SET @Load_ms += DateDiff(ms, @TimerDT, SYSDATETIME());
                END;
                
             SET @RowCount += 1;               

           END;

        COMMIT TRANSACTION;

        SET @MessageCount += 1;        
      
      END;

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
         , @Proc = ISNULL('Object' + SPACE(1) + ERROR_PROCEDURE() + SPACE(1), '')
         , @Line = ERROR_LINE()
         , @Message = CHAR(13) + CHAR(10) + ERROR_MESSAGE();

    IF XACT_STATE() <> 0 
      ROLLBACK TRANSACTION;

    RAISERROR ('Msg %d, Level %d, State %d, %s Line %d, %s'
              ,16 ,1 ,@Number, @Severity, @State, @Proc, @Line, @Message )

  END CATCH

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
    , MaxMessagesperReceive
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
    , @MessageQuantity
    , @RowsToPush
    , ReceiveMsgWaitTime 
    , NumberInitiatorTransactions
    , SendTxnWaitTime
    , NumberDialogs
    , DialogRecycleMaxMessages
    , MaxQueueReaders
    , DialogRecycleMaxMessages
    , LoggingLevel
    , RowVersioning
    , DataCompression
    , FillFactorPct
    , DateColumnIndexType
    , IdColumnIndexType
  FROM dbo.tTransferParameters p;

  RETURN CASE WHEN SCOPE_IDENTITY() = IDENT_CURRENT('dbo.tActivityHistory')
              THEN SCOPE_IDENTITY()
              ELSE 0
              END;

END;

GO

IF $(CONCURRENTCOUNT) = 1
  EXEC dbo.pGetIndexDetails 'SAMPLED'

GO
--EXEC dbo.$(LOADMETHOD) @RowsToPush = 10;
-- launch $(CONCURRENTCOUNT) concurrent SQLCMD threads 
:!!FOR /L %i IN (1,1,$(CONCURRENTCOUNT)) DO Start SQLCMD -SBILL764\R2 -E -d$(TARGETDB) -e -i "$(SCRIPTPATH)$(TESTSCRIPT)" -v LOADMETHOD=$(LOADMETHOD)
GO

IF $(CONCURRENTCOUNT) = 1
  BEGIN
    EXEC dbo.pGetActivityForLoadMethod '$(LOADMETHOD)$(ISOTYPE)$(LOCKTYPE)$(CONCURRENTCOUNT)';
    EXEC dbo.pGetIndexDetails 'SAMPLED';
  END 


GO 

USE master;
