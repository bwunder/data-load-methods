/*
UPDATE target.dbo.tTransferParameters SET RowsToPush = 10000
*/

:SETVAR LOADMETHOD "TVPSendBinary"
:SETVAR RUNNOW "True" -- True False      
:SETVAR TARGETDB "target"        -- must match target name in 1 - a - CREATE target.sql 
use $(TARGETDB);
GO
--------------------------------------------------------------------
-- send a table value parameter chocked full of rows
---------------------------------------------------------------------

SET NOCOUNT ON
GO

IF OBJECT_ID('dbo.$(LOADMETHOD)', 'P') IS NOT NULL
  DROP PROCEDURE dbo.$(LOADMETHOD)

GO

CREATE PROCEDURE dbo.$(LOADMETHOD)
AS
BEGIN

DECLARE @MessageQuantity BIGINT
      , @StartDT DATETIME2
      , @TableVariable TVPTableType
      , @NextRowNumber INT 
      , @RowsToPush INT 
      , @LoggingLevel TINYINT
      , @LoadMethod NVARCHAR(128)
      , @RowCount INT
      , @TimerDT DATETIME2
      , @LoadedRows BIGINT
      , @Load_ms BIGINT
      , @TVPToBinaryRows BIGINT
      , @TVPToBinary_ms BIGINT 
      , @Send_ms BIGINT 
      , @SumSend_ms BIGINT
      , @bigbin VARBINARY(MAX)
      , @ActivityHistoryId BIGINT;

SET @StartDt = SYSDATETIME(); 
SET @LoadMethod = '$(LOADMETHOD)';  

-- logging
SET @LoadedRows = 0;
SET @Load_ms = 0;
SET @Send_ms = 0;

SET @NextRowNumber = 1;
SET @MessageQuantity = (SELECT MessageQuantity FROM dbo.tTransferParameters);
SET @RowsToPush = (SELECT RowsToPush FROM dbo.tTransferParameters);
SET @LoggingLevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);

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
FROM dbo.fGetTableVariable(1, @RowsToPush * @MessageQuantity);

SET @RowCount = @@ROWCOUNT;

IF @RowCount > 0
  BEGIN
    IF @LoggingLevel>0
      SET @TimerDT = SySDATETIME();

    SET @bigbin = dbo.fBinaryMarshalTVP (@TableVariable)

    IF @LoggingLevel > 0
      SET @TVPToBinary_ms += DATEDIFF(ms, @TimerDT, SYSDATETIME());
    
    SET @NextRowNumber += @RowCount; 
      
    IF @LoggingLevel>0
      SET @TimerDT = SySDATETIME();
  
    EXEC dbo.SendBinaryDatagram @bigbin
                              , @LoadMethod
                              , @LoggingLevel
                              , @Send_ms OUTPUT;

    SET  @LoadedRows = CAST(SUBSTRING(@bigbin, 1, 8) AS BIGINT);

    IF @LoggingLevel > 0
      SET @Load_ms += DATEDIFF(ms, @TimerDT, SYSDATETIME());

    IF @LoggingLevel > 1
     BEGIN
       SELECT @SumSend_ms AS [Send_ms];
     END;

  END;

SET @LoadedRows = @Rowcount;   
    
WHILE @@TRANCOUNT > 0
  BEGIN
    SELECT 'oops - dangling transaction';
    ROLLBACK;
  END;

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

WHILE (1=1)
BEGIN
     SET @transmissioncount = (SELECT p.rows
                               FROM sys.objects AS o
                               JOIN sys.partitions AS p ON p.object_id = o.object_id
                               WHERE o.name = 'sysxmitqueue');
     IF @transmissioncount >= @lasttransmissioncount 
      BEGIN
        RAISERROR('no transmission queue progress - exit monitor loop',10,1);
        BREAK; 
      END                   
     SET @lasttransmissioncount = @transmissioncount; 
     IF @transmissioncount > 0
       SELECT @transmissioncount AS [Remaining Messages in transmission]; 
     IF (@transmissioncount = 0) BREAK;
     IF @RowsToPush < 1000
       WAITFOR DELAY '00:00:05:000'
     ELSE 
       WAITFOR DELAY '00:00:10:000';
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
 
RETURN @ActivityHistoryId;

END;

GO

IF '$(RUNNOW)' = 'True'

  BEGIN

    EXEC target.dbo.pGetBrokerStatus; 
    EXEC target.dbo.pGetQueueSizes;
    EXEC target.dbo.pGetIndexDetails 'SAMPLED'

    EXEC target.dbo.$(LOADMETHOD);

    -- give the receive a chance to finish before showing activity results
    -- the activation proc has a 5000ms wait, so for 10 rows 6 seconds should be enough
    -- if the receive data is still null manually run after the quere hits 0
    WAITFOR DELAY '00:00:06';

    EXEC target.dbo.pGetActivityForLoadMethod '$(LOADMETHOD)'
    -- target.dbo.pGetActivityForLoadMethod 'TVPSendBinary'
    -- select * from target..tReceiveHistory
    EXEC target.dbo.pGetIndexDetails 'SAMPLED';
    EXEC target.dbo.pGetQueueSizes;

  END; 

GO 

USE master;
