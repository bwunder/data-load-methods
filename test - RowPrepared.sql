
:SETVAR LOADMETHOD "RowPrepared" --  "RowPrepared_X3" for 3 concurrent                             
:SETVAR RUNNOW "True" -- True for RowPrepared, False if _X3, instead open in 3 query windows & run 
:SETVAR TARGETDB "target"        -- must match target name in 1 - a - CREATE target.sql            
:SETVAR TABLOCK "Tru"           -- "True" anything else is treated as false                       
SET NOCOUNT ON;
GO
use $(TARGETDB);
GO
---------------------------------------------------------------------
-- [MessageQuantity] sets of [RowsToPush] of singleton 
-- prepared statements are presented directly for Merge into   
-- a tParent/tChild hierarchy. The tChild will always get one 
-- row for each incoming row. The tParent will get a row if
-- the key is not found, otherwise a counters of matching 
-- occurrences of will be incremented. 
---------------------------------------------------------------------

IF OBJECT_ID('dbo.$(LOADMETHOD)', 'P') IS NOT NULL
  DROP PROCEDURE dbo.$(LOADMETHOD)

GO

CREATE PROCEDURE dbo.$(LOADMETHOD)
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
      , @LOBCheckSum INT;

DECLARE @MessageCount INT
      , @MessageQuantity BIGINT
      , @RowsToPush BIGINT
      , @RowCount INT
      , @EndDT DATETIME2
      , @LoggingLevel TINYINT
      , @LoadMethod NVARCHAR(128)
      , @StartDT DATETIME2 
      , @TimerDT DATETIME2 
      , @LoadedRows BIGINT
      , @Load_ms BIGINT
      , @MergeParentRows BIGINT
      , @MergeParent_ms BIGINT
      , @InsertChildRows BIGINT
      , @InsertChild_ms BIGINT
      , @SumMergeParentRows BIGINT
      , @SumMergeParent_ms BIGINT
      , @SumInsertChildRows BIGINT
      , @SumInsertChild_ms BIGINT
      , @PreparedStatement NVARCHAR(1000)
      , @PreparedParmList NVARCHAR(1000);

 SET @StartDt = SYSDATETIME(); 
 SET @LoadMethod = '$(LOADMETHOD)';  
 SET @PreparedStatement = N'EXEC dbo.pRowUpsert @RowNumber, @Host, @GMTDateTime, @Name, @Value, @FloatingPoint, @Boolean, @Bytes, @LOB, @LOBCheckSum' 
                        + ', @LoggingLevel, @LoadMethod'
                        + ', @MergeParentRows OUTPUT, @MergeParent_ms OUTPUT, @InsertChildRows OUTPUT, @InsertChild_ms OUTPUT;' 
 SET @PreparedParmList = N'@RowNumber INT, @Host NVARCHAR(128), @GMTDateTime DATETIME, @Name VARCHAR(50), @Value INT, @FloatingPoint REAL, @Boolean BIT, @Bytes VARBINARY(60), @LOB NVARCHAR(MAX), @LOBCheckSum INT' 
                       + N', @LoggingLevel TINYINT, @LoadMethod NVARCHAR(128)'
                       + N', @MergeParentRows BIGINT OUTPUT, @MergeParent_ms BIGINT OUTPUT, @InsertChildRows BIGINT OUTPUT, @InsertChild_ms BIGINT OUTPUT'

 -- logging
 SET @MessageCount = 0;
 SET @LoadedRows = 0;
 SET @Load_ms = 0;
 SET @MergeParentRows = 0;
 SET @MergeParent_ms = 0;
 SET @InsertChildRows = 0;
 SET @InsertChild_ms = 0;
 SET @SumMergeParentRows = 0;
 SET @SumMergeParent_ms = 0;
 SET @SumInsertChildRows = 0;
 SET @SumInsertChild_ms = 0;

 SET @LoggingLevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);
 SET @MessageQuantity = (SELECT MessageQuantity FROM dbo.tTransferParameters);
 SET @RowsToPush = (SELECT RowsToPush FROM dbo.tTransferParameters);

 WHILE @MessageCount < @MessageQuantity

  BEGIN                       

    SET @MessageCount += 1;
    SET @RowCount = 0
        
    BEGIN TRY
    
      BEGIN TRANSACTION AddBatchOfRows;
   
        IF '$(TABLOCK)' = 'True'  
          SELECT TOP(1) p.Id
          INTO #junk
          FROM dbo.tParent p WITH(TABLOCK,HOLDLOCK)
          JOIN dbo.tChild c WITH(TABLOCK,HOLDLOCK)
          ON p.Id = c.ParentId;

        WHILE @RowCount < @RowsToPush

          BEGIN                

            SET @RowCount += 1;

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
            FROM dbo.fGetTableVariable (@RowCount, 1); 

            IF @LoggingLevel > 0
              SET @TimerDT = SYSDATETIME();

            EXEC sp_executesql @PreparedStatement
                             , @PreparedParmList
                             , @RowNumber
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
                             , @LoadMethod
                             , @MergeParentRows OUTPUT
                             , @MergeParent_ms OUTPUT
                             , @InsertChildRows OUTPUT
                             , @InsertChild_ms OUTPUT;

            SET @LoadedRows += @InsertChildRows; 

            IF @LoggingLevel > 0
              SET @Load_ms += DATEDIFF(ms, @TimerDT, SYSDATETIME());
      
            SET @SumMergeParentRows += @MergeParentRows;
            SET @SumMergeParent_ms += @MergeParent_ms;
            SET @SumInsertChildRows += @InsertChildRows;
            SET @SumInsertChild_ms += @InsertChild_ms; 

         END;

       COMMIT TRANSACTION AddBatchOfRows;

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
             , @Message = CHAR(13) + CHAR(10) + ERROR_MESSAGE();

        IF XACT_STATE() <> 0 
           ROLLBACK TRANSACTION AddBatchOfRows;

        RAISERROR ('Msg %d, Level %d, State %d, %s Line %d %s'
                  ,16 ,1 ,@Number, @Severity, @State, @Proc, @Line, @Message)


    END CATCH
 
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
  SELECT @LoadMethod + CASE WHEN '$(TABLOCK)' = 'true' 
                            THEN '' 
                            ELSE 'NOTABLOCK' END
       , @StartDT 
       , @SumInsertChildRows
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

    IF '$(TABLOCK)' = 'true'
      EXEC dbo.pGetActivityForLoadMethod '$(LOADMETHOD)';
    ELSE
      EXEC dbo.pGetActivityForLoadMethod '$(LOADMETHOD)NOTABLOCK';
    EXEC dbo.pGetIndexDetails 'SAMPLED';

  END; 
  
GO 

USE master;

