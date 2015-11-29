/*
-- when 1 row is sent the activation proc thinks it is a RowSendXML
-- that means it is not seeing the wrapper
UPDATE target.dbo.tTransferParameters SET RowsToPush = 10
*/

:SETVAR LOADMETHOD "TVPSendXML"                                                         
:SETVAR RUNNOW "True"            -- True False                                          
:SETVAR TARGETDB "target"        -- must match target name in 1 - a - CREATE target.sql 
:SETVAR INITIATORDB "initiator"  -- must match target name in 1 - a - CREATE target.sql 
use $(INITIATORDB);
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

DECLARE @StartDT DATETIME2
      , @LoadMethod NVARCHAR(128)
      , @MessageQuantity INT 
      , @RowsToPush INT 
      , @LoggingLevel TINYINT
      , @LoadedRows BIGINT
      , @Load_ms BIGINT
      , @Send_ms BIGINT
      , @TimerDT DATETIME2
      , @TableVariable TVPTableType
      , @Payload XML
      , @ActivityHistoryId INT;

SET @StartDt = SYSDATETIME(); 
SET @LoadMethod = '$(LOADMETHOD)';  

SET @RowsToPush = (SELECT RowsToPush FROM dbo.tTransferParameters);
SET @LoggingLevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);
SET @MessageQuantity = (SELECT MessageQuantity FROM dbo.tTransferParameters);

BEGIN TRY

  -- need a primary key to generate the XML
  -- so need to go from keyed table to XML instead of direct to XML
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
  FROM dbo.fGetTableVariable(1, @RowsToPush);

  SET @LoadedRows = @@ROWCOUNT;

  IF @LoggingLevel > 0
    SET @TimerDT = SYSDATETIME();

  SET @Payload = (SELECT '<rows LoadMethod="$(LOADMETHOD)" Count=' + CAST(@GeneratedRows AS VARCHAR(10)) + '>'
                 + (SELECT RowNumber
                         , Host
                         , GMTDateTime
                         , Name
                         , Value
                         , FloatingPoint
                         , Boolean
                         , Bytes
                         , LOB 
                         , LOBCheckSum
                    FROM @TableVariable AS [row]
                    FOR XML AUTO)
                + '</rows>');

  IF @LoggingLevel>0
    SET @TimerDT = SySDATETIME();

  EXEC dbo.SendXMLPayload @Payload
                        , @LoadMethod
                        , @LoggingLevel
                        , @Send_ms OUTPUT;

  IF @LoggingLevel > 0
    SET @Load_ms = DATEDIFF(ms, @TimerDT, SYSDATETIME());

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

IF @LoggingLevel > 0 
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

  IF @LoggingLevel > 1
   BEGIN
     SELECT @Send_ms AS [Send_ms];
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
     IF @transmissioncount >= @lasttransmissioncount 
      BEGIN
        RAISERROR('transmission queue stagnant - exit monitor loop',10,1);
        BREAK; 
      END                   
     SET @lasttransmissioncount = @transmissioncount; 
     IF @transmissioncount > 0
       SELECT @transmissioncount AS [Remaining Messages in transmission]; 
     IF (@transmissioncount = 0) BREAK;
     WAITFOR DELAY '00:00:05:000';
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

IF EXISTS (SELECT * FROM [$(TARGETDB)].[dbo].[tTargetProcessingErrors] WHERE CreateDT > @StartDT)
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
FROM [$(TARGETDB)].[dbo].[tTargetProcessingErrors] 
WHERE CreateDT > @StartDT;
 
RETURN @ActivityHistoryId;

END;

GO

IF '$(RUNNOW)' = 'True'

  BEGIN

    EXEC dbo.pGetBrokerStatus; 
    EXEC dbo.pGetQueueSizes;
    EXEC dbo.pGetIndexDetails 'SAMPLED'

    EXEC dbo.$(LOADMETHOD);

    -- better if a little longer than RECEIVE wait 
    WAITFOR DELAY '00:00:06';

    EXEC dbo.pGetActivityForLoadMethod '$(LOADMETHOD)'
    EXEC dbo.pGetIndexDetails 'SAMPLED';
    EXEC dbo.pGetQueueSizes;

  END; 

GO 

USE master;
