/*
-- this one blows up if requested rows > available rows
UPDATE target.dbo.tTransferParameters SET RowsToPush = 10
*/

:SETVAR LOADMETHOD "RowSendBinary"
:SETVAR RUNNOW "True" -- True False      
:SETVAR TARGETDB "target"        -- must match target name in 1 - a - CREATE target.sql 
:SETVAR INITIATORDB "initiator"     -- must match target name in 1 - c - CREATE initiator.sql 
use $(TARGETDB);

--------------------------------------------------------------------
-- send one row at a time as XML to a Service Broker queue 
---------------------------------------------------------------------
SET NOCOUNT ON
GO

IF OBJECT_ID('dbo.$(LOADMETHOD)', 'P') IS NOT NULL
  DROP PROCEDURE dbo.$(LOADMETHOD)

GO

CREATE PROCEDURE dbo.$(LOADMETHOD)
AS
BEGIN

DECLARE @MessageCount INT 
      , @FirstRow INT 
      , @RowCount INT 
      , @LoggingLevel INT  
      , @LoadMethod NVARCHAR(128)
      , @MessageQuantity BIGINT
      , @RowsToPush INT
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
      , @bigbin VARBINARY(MAX)
      , @TVPToBinaryRows BIGINT
      , @TVPToBinary_ms BIGINT
      , @Send_ms BIGINT
      , @SumTVPToBinaryRows BIGINT
      , @SumTVPToBinary_ms BIGINT
      , @SumSend_ms BIGINT 
      , @StartDT DATETIME2
      , @TimerDT DATETIME2
      , @LoadedRows BIGINT
      , @Load_ms BIGINT
      , @SourceData TVPTableType
      , @TableVariable TVPTableType
      , @Datagram VARBINARY(MAX)
      , @ActivityHistoryId INT;

-- we need a temp table not a table variable here (wide rows in example)
-- needs to look like TVPTableType but to much work to get an index on after SELECT INTO
SET @LoadMethod = '$(LOADMETHOD)';
SET @StartDt = SYSDATETIME();

SET @FirstRow = 1;

SET @LoadedRows = 0;
SET @Load_ms = 0;
SET @SumSend_ms = 0;

SET @LoggingLevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);
SET @MessageQuantity = (SELECT MessageQuantity FROM dbo.tTransferParameters);
SET @RowsToPush = (SELECT RowsToPush FROM dbo.tTransferParameters);

SET @MessageCount = 0;

WHILE @MessageCount < (SELECT MessageQuantity FROM dbo.tTransferParameters) 
  BEGIN
    SET @MessageCount += 1;
     
    -- the rowcount is first in thebigbin
    SET @bigbin = CAST(@GeneratedRows AS BIGINT) 

    SET @RowCount = 0
    WHILE  @RowCount < @RowsToPush
      BEGIN
   
        SET @RowCount += 1; 
         
        SET @Datagram =  
         (SELECT dbo.fBinaryMarshalRow([RowNumber]
                                     , [Host]
                                     , [GMTDateTime]
                                     , [Name]
                                     , [Value]
                                     , [FloatingPoint]
                                     , [Boolean]
                                     , [Bytes]
                                     , [LOB]
                                     , [LOBCheckSum])
          FROM dbo.fGetTableVariable (((@MessageCount -1 ) * @RowsToPush) + @RowCount, 1)); 

        IF @LoggingLevel > 0
          SET @TimerDT = SYSDATETIME();

        EXEC dbo.SendBinaryDatagram  @Datagram
                                   , @LoadMethod
                                   , @LoggingLevel
                                   , @Send_ms;

        IF @LoggingLevel > 0
          SET @Load_ms += DATEDIFF(ms, @TimerDt, SYSDATETIME());

        SET @SumSend_ms += @Send_ms;
        SET @LoadedRows += 1;-- running tally 

      END; 

  END; 

needs try-catch

WHILE @@TRANCOUNT > 0
 BEGIN
  SELECT 'oops - dangling transaction';
  ROLLBACK;
 END;

-- until this identity is laid down, the receive cannot match it 
INSERT target.dbo.tActivityHistory
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
     IF @transmissioncount >= @lasttransmissioncount 
       BEGIN
         RAISERROR('Transmission queue message count stagnant, check queues!',10,1)
         BREAK; 
       END                   
     SET @lasttransmissioncount = @transmissioncount 
     IF @transmissioncount > 0
       SELECT @transmissioncount AS [Messages In Transmission]; 
     WAITFOR DELAY '00:00:05:000';
END;

IF EXISTS (SELECT * FROM [target].[dbo].[initiator_processing_errors] WHERE CreateDT > @StartDT)
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
  FROM [target].[dbo].[initiator_processing_errors] 
  WHERE CreateDT > @StartDT;

IF EXISTS (SELECT * FROM [target].[dbo].[unsent_messages] WHERE CreateDT > @StartDT)
  SELECT 
      [Id]
    , message_type_name
    , message_body
    , CreateDt
  FROM [target].[dbo].[unsent_messages] WHERE CreateDT > @StartDT;

IF EXISTS (SELECT * FROM [target].[dbo].[tTargetProcessingErrors] WHERE CreateDT > @StartDT)
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
 FROM [target].[dbo].[tTargetProcessingErrors] 
 WHERE CreateDT > @StartDT;

IF @LoggingLevel > 1
 SELECT @SumSend_ms AS [Send_ms]

RETURN @ActivityHistoryId;

END;

GO

IF '$(RUNNOW)' = 'True'

  BEGIN

    EXEC target.dbo.pGetBrokerStatus; 
    EXEC target.dbo.pGetQueueSizes;
    EXEC target.dbo.pGetIndexDetails 'SAMPLED'

    EXEC target.dbo.$(LOADMETHOD);

    WAITFOR DELAY '00:00:06';

    EXEC target.dbo.pGetActivityForLoadMethod '$(LOADMETHOD)'
    /*
       target.dbo.pGetActivityForLoadMethod 'RowSendBinary'
       select * from target..tActivityHistory where loadmethod = 'RowSendBinary'
       select * from target..tReceiveHistory where loadmethod = 'RowSendBinary'
 update target..tActivityHistory
 set Generate_ms = 0, GeneratedRows = 0 where loadmethod = 'RowSendBinary'
    */
    EXEC target.dbo.pGetIndexDetails 'SAMPLED';
    EXEC target.dbo.pGetQueueSizes;

                                              
  END; 

GO 

USE master;
