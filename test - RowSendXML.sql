
:SETVAR LOADMETHOD "RowSendXML"                                                               
:SETVAR RUNNOW "True" -- True False                                                           
:SETVAR TARGETDB "target"           -- must match target name in 1 - a - CREATE target.sql    
:SETVAR INITIATORDB "initiator"     -- must match target name in 1 - c - CREATE initiator.sql 
SET NOCOUNT ON
GO
--------------------------------------------------------------------
-- send one row at a time as XML to a Service Broker queue 
---------------------------------------------------------------------
use $(INITIATORDB);
GO

IF OBJECT_ID('dbo.$(LOADMETHOD)', 'P') IS NOT NULL
  DROP PROCEDURE dbo.$(LOADMETHOD)

GO

CREATE PROCEDURE dbo.$(LOADMETHOD)
AS
BEGIN

DECLARE @MessageCount INT 
      , @RowCount INT 
      , @MessageQuantity INT 
      , @RowsToPush INT 
      , @LoggingLevel INT  
      , @LoadMethod NVARCHAR(128)
      , @RowNumber INT
      , @Host NVARCHAR(128)
      , @GMTDateTime DATETIME
      , @Name VARCHAR(50)
      , @Value INT 
      , @FloatingPoint REAL
      , @Boolean BIT
      , @Bytes VARBINARY(60)
      , @LOB NVARCHAR(MAX)
      , @LOBCheckSum INT
      , @Payload XML
      , @Send_ms BIGINT 
      , @SumSend_ms BIGINT 
      , @StartDT DATETIME2
      , @TimerDT DATETIME2
      , @LoadedRows BIGINT
      , @Load_ms BIGINT
      , @ActivityHistoryId INT;

SET @LoadMethod = '$(LOADMETHOD)';
SET @StartDT = SYSDATETIME();

SET @LoggingLevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);
SET @MessageQuantity = (SELECT MessageQuantity FROM dbo.tTransferParameters);
SET @RowsToPush = (SELECT RowsToPush FROM dbo.tTransferParameters);

SET @LoadedRows = 0;
SET @Load_ms = 0;
SET @SumSend_ms = 0;

SET @MessageCount = 0;
WHILE @MessageCount < @MessageQuantity 
  BEGIN

    SET @MessageCount += 1;

    SET @RowCount = 0;
    WHILE @RowCount < @RowsToPush 
      BEGIN
 
        SET @RowCount += 1; 

        SELECT @RowNumber = [RowNumber]
             , @Host = [Host]
             , @GMTDateTime = [GMTDateTime]
             , @Name = [Name]
             , @Value = [Value]
             , @FloatingPoint = [FloatingPoint]
             , @Boolean = [Boolean]
             , @Bytes = [Bytes]
             , @LOB = [LOB]
             , @LOBCheckSum = [LOBCheckSum]
        FROM dbo.SourceData
        WHERE RowNumber = (((@MessageCount - 1) * @RowsToPush) + @RowCount); 

        SET @Payload = (SELECT '<rows LoadMethod="$(LOADMETHOD)" Count="1">'
                            + (SELECT [RowNumber]
                                    , [Host]
                                    , [GMTDateTime]
                                    , [Name]
                                    , [Value]
                                    , [FloatingPoint]
                                    , [Boolean]
                                    , [Bytes]
                                    , [LOB]
                                    , [LOBCheckSum]  
                                FROM (SELECT @RowNumber AS [RowNumber]
                                           , @Host AS [Host]
                                           , @GMTDateTime AS [GMTDateTime]
                                           , @Name AS [Name]
                                           , @Value AS [Value]
                                           , @FloatingPoint AS [FloatingPoint]
                                           , @Boolean AS [Boolean]
                                           , CONVERT(VARCHAR(MAX),@Bytes,1) AS [Bytes]
                                           , @LOB AS [LOB] 
                                           , @LOBCheckSum AS [LOBCheckSum]) AS row 
                                       FOR XML AUTO) 
                              + '</rows>');
     
        SET @Send_ms = 0;

        IF @LoggingLevel > 0
          SET @TimerDT = SYSDATETIME();

        EXEC dbo.SendXMLPayload   @Payload
                                , @LoadMethod
                                , @LoggingLevel
                                , @Send_ms OUTPUT;
             
        IF @LoggingLevel > 0
          SET @Load_ms += DATEDIFF(ms, @TimerDT, SYSDATETIME());

        SET @SumSend_ms += @Send_ms;
        SET @LoadedRows += 1;   

      END; 
  
  END; 

WHILE @@TRANCOUNT > 0
 BEGIN
  SELECT 'oops - dangling transaction';
  ROLLBACK;
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

SET @ActivityHistoryId = CASE WHEN SCOPE_IDENTITY() = IDENT_CURRENT('dbo.tActivityHistory')
                              THEN SCOPE_IDENTITY()
                              ELSE 0
                              END;

---------------------------------------------------------------------
-- Wait for transmission queue to be empty, signifying the
-- reception and acknowledgement of all messages by the target.
-- Use this efficient checking method every 5 seconds.
---------------------------------------------------------------------
DECLARE @transmissioncount BIGINT, @lasttransmissioncount BIGINT;
SET @lasttransmissioncount = 200000000000000;

WHILE (1=1)
BEGIN
     SET @transmissioncount = (SELECT p.rows
                               FROM sys.objects AS o
                               JOIN sys.partitions AS p ON p.object_id = o.object_id
                               WHERE o.name = 'sysxmitqueue');
     -- the example like this test down by the WAITFOR, I think it works better up here
     IF (@transmissioncount = 0)       
       BREAK;
     ELSE IF @transmissioncount = @lasttransmissioncount 
       BEGIN
         RAISERROR('transmission queue count stagnant!',10,1)
         BREAK; 
       END                   
     SET @lasttransmissioncount = @transmissioncount 
     IF @transmissioncount > 0
       SELECT @transmissioncount AS [Messages In Transmission]; 
     WAITFOR DELAY '00:00:06:000';
END;

IF EXISTS (SELECT * FROM [dbo].[initiator_processing_errors] WHERE CreateDT > @StartDT)
  SELECT  [Id] AS [initiator_processing_error_id]
        , [error_conversation] 
        , [error_number]
        , [error_message]
        , [error_severity]
        , [error_state]
        , [error_procedure]
        , [error_line]
        , [doomed_transaction] 
        , [error_xml]
  FROM [dbo].[initiator_processing_errors] 
  WHERE CreateDT > @StartDT;

IF EXISTS (SELECT * FROM [dbo].[unsent_messages] WHERE CreateDT > @StartDT)
  SELECT 
      [Id]
    , message_type_name
    , message_body
    , CreateDt
  FROM [dbo].[unsent_messages] WHERE CreateDT > @StartDT;

IF EXISTS (SELECT * FROM [dbo].[tTargetProcessingErrors] WHERE CreateDT > @StartDT)
  SELECT   [Id] AS [target_processing_error_Id]
         , [error_conversation] 
         , [error_number]
         , [error_message]
         , [error_severity]
         , [error_state]
         , [error_procedure]
         , [error_line]
         , [doomed_transaction] 
         , [error_xml]                         
  FROM [dbo].[tTargetProcessingErrors] 
  WHERE CreateDT > @StartDT;

RETURN @ActivityHistoryId;

END;

GO

IF '$(RUNNOW)' = 'True'

  BEGIN
    
    EXEC dbo.pGetBrokerStatus; 
    EXEC dbo.pGetQueueSizes;
    EXEC dbo.pGetIndexDetails 'SAMPLED';

    UPDATE dbo.tTransferParameters SET RowsToPush = 10;
    EXEC dbo.$(LOADMETHOD);
    --UPDATE dbo.tTransferParameters SET RowsToPush = 50;
    --EXEC dbo.$(LOADMETHOD);
    --UPDATE dbo.tTransferParameters SET RowsToPush = 100;
    --EXEC dbo.$(LOADMETHOD);
    --UPDATE dbo.tTransferParameters SET RowsToPush = 500;
    --EXEC dbo.$(LOADMETHOD);
    --UPDATE dbo.tTransferParameters SET RowsToPush = 1000;
    --EXEC dbo.$(LOADMETHOD);
    --UPDATE dbo.tTransferParameters SET RowsToPush = 5000;
    --EXEC dbo.$(LOADMETHOD);
    --UPDATE dbo.tTransferParameters SET RowsToPush = 10000;
    --EXEC dbo.$(LOADMETHOD);
    --UPDATE dbo.tTransferParameters SET RowsToPush = 50000;
    --EXEC dbo.$(LOADMETHOD);
    --UPDATE dbo.tTransferParameters SET RowsToPush = 100000;
    --EXEC dbo.$(LOADMETHOD);
 
    WAITFOR DELAY '00:00:06';

    EXEC dbo.pGetActivityForLoadMethod '$(LOADMETHOD)'
    EXEC dbo.pGetIndexDetails 'SAMPLED';
    EXEC dbo.pGetQueueSizes;

  END; 

GO 

USE master;
