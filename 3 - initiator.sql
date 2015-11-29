-- SQLCMD substitution variables - always wrap in double quotes
:SETVAR INITIATORDB "initiator"                                                                                       
:SETVAR TARGETDB "target"                     -- must match target name in 1 - a - CREATE target.sql                  
:SETVAR TARGETSQLINSTANCE "BILL764\ATER2"     -- (SERVER\INSTANCE)leave blank if target is on local SQL Instance      
:SETVAR DATAPATH "C:\Program Files\Microsoft SQL Server\MSSQL10_50.ATER2\MSSQL\DATA" --"G:\MSSQL10_50.R2\MSSQL\DATA\" 
:SETVAR LOGPATH "C:\Program Files\Microsoft SQL Server\MSSQL10_50.ATER2\MSSQL\DATA"--"G:\MSSQL10_50.R2\MSSQL\DATA\"   

:SETVAR ROWVERSIONING "OFF"      -- "ON" "OFF"                                                                        
:SETVAR DATACOMPRESSION "ROW"    -- "ROW" "PAGE" "NONE"                                                               

-- broker config
:SETVAR DOMAINNAME ""            -- leave blank if no domain                                                          
:SETVAR ENCRYPTION "DISABLED"    -- "ENABLED" "DISABLED" default is enabled!                                          
:SETVAR INITIATORHOST "BILL_VU"  -- server name of initiator db service host, server name only, NO SQL instance name  
:SETVAR INITIATORPORT 4022       -- 4022 is the one SQL Server uses if port not specified                             
:SETVAR TARGETHOST "BILL764"     -- server name of initiator db service host, server name only, NO SQL instance name  
:SETVAR TARGETPORT 4022          -- 4022 is the one SQL Server uses if port not specified                             


SET NOCOUNT ON;
GO
USE master;
GO

IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '$(TARGETDB)')
  RAISERROR('%s db must be created before %s db!',20,1,'$(TARGETDB)','$(INITIATORDB)') WITH LOG

GO
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '$(INITIATORDB)')
  BEGIN
    
    CREATE DATABASE $(INITIATORDB)
    ON ( NAME = $(INITIATORDB),
         FILENAME = '$(DATAPATH)\$(INITIATORDB).mdf',
         SIZE = 500MB,
         MAXSIZE = 1000MB,
         FILEGROWTH = 100MB )
    LOG ON ( NAME = $(INITIATORDB)_log,
             FILENAME = '$(LOGPATH)\$(INITIATORDB)_log.ldf',
             SIZE = 20MB,
             MAXSIZE = 100MB,
             FILEGROWTH = 10MB);

    ALTER DATABASE $(INITIATORDB)
    SET RECOVERY SIMPLE;

    ALTER DATABASE $(INITIATORDB)
    SET READ_COMMITTED_SNAPSHOT $(ROWVERSIONING);

  END;
  
GO

use $(INITIATORDB)

GO

IF EXISTS(SELECT * FROM sys.databases
          WHERE database_id = DB_ID()
          AND name = '$(INITIATORDB)'
          AND owner_sid <> 0x01)
  EXEC sp_changedbowner 'sa'

GO

------------ Type that defines the source data --------------'
-- cannot drop a type w/out first removing all reference it

IF NOT EXISTS (SELECT * FROM sys.types
               WHERE name = 'TVPTableType')
  CREATE TYPE TVPTableType AS TABLE 
      ( RowNumber INT PRIMARY KEY
      , Host NVARCHAR(128)
      , GMTDateTime DATETIME
      , Name VARCHAR(50)
      , Value INT 
      , FloatingPoint REAL
      , Boolean BIT
      , Bytes VARBINARY(60)
      , LOB NVARCHAR(MAX)
      , LOBCheckSum INT);

GO

IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'tTransferParameters')
  DROP SYNONYM [dbo].[tTransferParameters]
GO
IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'tTargetProcessingErrors')
  DROP SYNONYM [dbo].[tTargetProcessingErrors]
GO
IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'tActivityHistory')
  DROP SYNONYM [dbo].tActivityHistory
GO
IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'tReceiveHistory')
  DROP SYNONYM [dbo].tReceiveHistory
GO
IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'pRowUpsert')
  DROP SYNONYM [dbo].[pRowUpsert]
GO
IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'fGetTableVariable')
  DROP SYNONYM [dbo].[fGetTableVariable]
GO
IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'pGetIndexDetails')
  DROP SYNONYM [dbo].[pGetIndexDetails]
GO
IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'pGetQueueSizes')
  DROP SYNONYM [dbo].[pGetQueueSizes]
GO
IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'pGetActivityForLoadMethod')
  DROP SYNONYM [dbo].[pGetActivityForLoadMethod]
GO
IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'pGetBrokerStatus')
  DROP SYNONYM [dbo].[pGetBrokerStatus]
GO

IF '$(TARGETDB)' <> '$(INITIATORDB)'
    IF '$(TARGETSQLINSTANCE)' NOT IN ('', @@SERVERNAME)
      BEGIN
        EXEC('CREATE SYNONYM [dbo].[tTransferParameters] FOR [$(TARGETSQLINSTANCE)].[$(TARGETDB)].[dbo].[tTransferParameters]');
        EXEC('CREATE SYNONYM [dbo].[tTargetProcessingErrors] FOR [$(TARGETSQLINSTANCE)].[$(TARGETDB)].[dbo].[tTargetProcessingErrors]');
        EXEC('CREATE SYNONYM [dbo].[tActivityHistory] FOR [$(TARGETSQLINSTANCE)].[$(TARGETDB)].[dbo].[tActivityHistory]');
        EXEC('CREATE SYNONYM [dbo].[tReceiveHistory] FOR [$(TARGETSQLINSTANCE)].[$(TARGETDB)].[dbo].[tReceiveHistory]');
        EXEC('CREATE SYNONYM [dbo].[pRowUpsert] FOR [$(TARGETSQLINSTANCE)].[$(TARGETDB)].[dbo].[pRowUpsert]');
        EXEC('CREATE SYNONYM [dbo].[fGetTableVariable] FOR [$(TARGETSQLINSTANCE)].[$(TARGETDB)].[dbo].[fGetTableVariable]');
        EXEC('CREATE SYNONYM [dbo].[pGetIndexDetails] FOR [$(TARGETSQLINSTANCE)].[$(TARGETDB)].[dbo].[pGetIndexDetails]');
        EXEC('CREATE SYNONYM [dbo].[pGetQueueSizes] FOR [$(TARGETSQLINSTANCE)].[$(TARGETDB)].[dbo].[pGetQueueSizes]');
        EXEC('CREATE SYNONYM [dbo].[pGetActivityForLoadMethod] FOR [$(TARGETSQLINSTANCE)].[$(TARGETDB)].[dbo].[pGetActivityForLoadMethod]');
        EXEC('CREATE SYNONYM [dbo].[pGetBrokerStatus] FOR [$(TARGETSQLINSTANCE)].[$(TARGETDB)].[dbo].[GetBroketrStatus]');
      END 
    ELSE
      BEGIN
        CREATE SYNONYM [dbo].[tTransferParameters] FOR [$(TARGETDB)].[dbo].[tTransferParameters];
        CREATE SYNONYM [dbo].[tTargetProcessingErrors] FOR [$(TARGETDB)].[dbo].[tTargetProcessingErrors];
        CREATE SYNONYM [dbo].[tActivityHistory] FOR [$(TARGETDB)].[dbo].[tActivityHistory];
        CREATE SYNONYM [dbo].[tReceiveHistory] FOR [$(TARGETDB)].[dbo].[tReceiveHistory];
        CREATE SYNONYM [dbo].[pRowUpsert] FOR [$(TARGETDB)].[dbo].[pRowUpsert]
        CREATE SYNONYM [dbo].[fGetTableVariable] FOR [$(TARGETDB)].[dbo].[fGetTableVariable];
        CREATE SYNONYM [dbo].[pGetIndexDetails] FOR [$(TARGETDB)].[dbo].[pGetIndexDetails];
        CREATE SYNONYM [dbo].[pGetQueueSizes] FOR [$(TARGETDB)].[dbo].[pGetQueueSizes];
        CREATE SYNONYM [dbo].[pGetActivityForLoadMethod] FOR [$(TARGETDB)].[dbo].[pGetActivityForLoadMethod];
        CREATE SYNONYM [dbo].[pGetBrokerStatus] FOR [$(TARGETDB)].[dbo].[pGetBrokerStatus];
      END; 
GO

-- only drop this if the db is dropped
IF OBJECT_ID('dbo.SourceData','U') IS NULL
  CREATE TABLE dbo.SourceData
      ( RowNumber INT IDENTITY(1,1) 
      , Host NVARCHAR(128)
      , GMTDateTime DATETIME
      , Name VARCHAR(50)
      , Value INT 
      , FloatingPoint REAL
      , Boolean BIT
      , Bytes VARBINARY(60)
      , LOB NVARCHAR(MAX)
      , LOBCheckSum INT
      , CONSTRAINT pkc_SourceData__RowNumber
        PRIMARY KEY (RowNumber))
  WITH (DATA_COMPRESSION = $(DATACOMPRESSION));
 
GO

-- repeat the jumbled inserts until there is over 100,000 rows
WHILE (SELECT COUNT(*) FROM dbo.SourceData) < 100000

BEGIN
-- add the contents of the ring buffer in buffer date order 
INSERT dbo.SourceData
    ( Host
    , GMTDateTime
    , Name
    , Value 
    , FloatingPoint
    , Boolean
    , Bytes
    , LOB
    , LOBCheckSum )
SELECT @@SERVERNAME AS [Host]
     , DATEADD ( hour, DATEDIFF( hour, SYSDATETIME(), SYSUTCDATETIME()), [Entry].OccurredAt) AS [GMTDateTime]
     , [Entry].[Name]
     , [Enqueued].[History].[value] ('@id','int') AS [Value]
     , CAST(@@PROCID AS Real)/([Enqueued].[History].value('@id','int') + 1) AS [FloatingPoint]
     , CASE WHEN DATEDIFF( d, [Entry].[OccurredAt], SYSDATETIME()) > 1 THEN 0 ELSE 1 END AS [Boolean]
     , [Bytes] 
     , CAST([Entry].[xmlDoc] AS NVARCHAR(MAX)) AS [LOB]
     , [LOBCheckSum]
FROM (SELECT ROW_NUMBER () OVER (ORDER BY b.[Timestamp]) AS [RowNumber]
            , CAST(Record AS XML) AS xmlDoc
            , DATEADD( ms
                     , -1 * i. cpu_ticks /(i.cpu_ticks/i.ms_ticks ) 
                       + b.[Timestamp]
                     , SYSDATETIME()) AS [OccurredAt] 
           , [ring_buffer_type] AS [Name]
           , [ring_buffer_Address] AS [Bytes]
           , CHECKSUM(Record) AS [LOBCheckSum]
      FROM sys.dm_os_ring_buffers b
      CROSS JOIN sys.dm_os_sys_info i) AS [Entry]
CROSS APPLY xmlDoc.nodes('//Record') AS [Enqueued]([History])  
ORDER BY GMTDateTime;
-- then in name order  
INSERT dbo.SourceData
    ( Host
    , GMTDateTime
    , Name
    , Value 
    , FloatingPoint
    , Boolean
    , Bytes
    , LOB
    , LOBCheckSum )
SELECT @@SERVERNAME AS [Host]
     , DATEADD ( hour, DATEDIFF( hour, SYSDATETIME(), SYSUTCDATETIME()), [Entry].OccurredAt) AS [GMTDateTime]
     , [Entry].[Name]
     , [Enqueued].[History].[value] ('@id','int') AS [Value]
     , CAST(@@PROCID AS Real)/([Enqueued].[History].value('@id','int') + 1) AS [FloatingPoint]
     , CASE WHEN DATEDIFF( d, [Entry].[OccurredAt], SYSDATETIME()) > 1 THEN 0 ELSE 1 END AS [Boolean]
     , [Bytes] 
     , CAST([Entry].[xmlDoc] AS NVARCHAR(MAX)) AS [LOB]
     , [LOBCheckSum]
FROM (SELECT ROW_NUMBER () OVER (ORDER BY b.[Timestamp]) AS [RowNumber]
            , CAST(Record AS XML) AS xmlDoc
            , DATEADD( ms
                     , -1 * i. cpu_ticks /(i.cpu_ticks/i.ms_ticks ) 
                       + b.[Timestamp]
                     , SYSDATETIME()) AS [OccurredAt] 
           , [ring_buffer_type] AS [Name]
           , [ring_buffer_Address] AS [Bytes]
           , DATALENGTH(Record) AS [LOBCheckSum]
      FROM sys.dm_os_ring_buffers b
      CROSS JOIN sys.dm_os_sys_info i) AS [Entry]
CROSS APPLY xmlDoc.nodes('//Record') AS [Enqueued]([History])  
ORDER BY Name;
-- and again in nonsense order
INSERT dbo.SourceData
    ( Host
    , GMTDateTime
    , Name
    , Value 
    , FloatingPoint
    , Boolean
    , Bytes
    , LOB
    , LOBCheckSum )
SELECT @@SERVERNAME AS [Host]
     , DATEADD ( hour, DATEDIFF( hour, SYSDATETIME(), SYSUTCDATETIME()), [Entry].OccurredAt) AS [GMTDateTime]
     , [Entry].[Name]
     , [Enqueued].[History].[value] ('@id','int') AS [Value]
     , CAST(@@PROCID AS Real)/([Enqueued].[History].value('@id','int') + 1) AS [FloatingPoint]
     , CASE WHEN DATEDIFF( d, [Entry].[OccurredAt], SYSDATETIME()) > 1 THEN 0 ELSE 1 END AS [Boolean]
     , [Bytes] 
     , CAST([Entry].[xmlDoc] AS NVARCHAR(MAX)) AS [LOB]
     , [LOBCheckSum]
FROM (SELECT ROW_NUMBER () OVER (ORDER BY b.[Timestamp]) AS [RowNumber]
            , CAST(Record AS XML) AS xmlDoc
            , DATEADD( ms
                     , -1 * i. cpu_ticks /(i.cpu_ticks/i.ms_ticks ) 
                       + b.[Timestamp]
                     , SYSDATETIME()) AS [OccurredAt] 
           , [ring_buffer_type] AS [Name]
           , [ring_buffer_Address] AS [Bytes]
           , DATALENGTH(Record) AS [LOBCheckSum]
      FROM sys.dm_os_ring_buffers b
      CROSS JOIN sys.dm_os_sys_info i) AS [Entry]
CROSS APPLY xmlDoc.nodes('//Record') AS [Enqueued]([History])  
ORDER BY LOBCheckSum, GMTDateTime DESC;

END;

GO
IF OBJECT_ID('dbo.SendBinaryDatagram', 'P') IS NOT NULL
    DROP PROCEDURE dbo.SendBinaryDatagram;

GO
-- send row-blobs as service broker messages.
CREATE PROCEDURE dbo.SendBinaryDatagram
  ( @Datagram VARBINARY(MAX) 
  , @LoadMethod VARCHAR(128)
  , @LoggingLevel TINYINT 
  , @Send_ms BIGINT OUTPUT) 
AS
BEGIN
    SET NOCOUNT ON;

    -- Get initiator parameters.
    DECLARE @MessageQuantity BIGINT;
    DECLARE @RowsToPush INT;
    DECLARE @NumberInitiatorTransactions INT;
    DECLARE @SendTxnWaitTime CHAR(12);
    DECLARE @NumberDialogs INT;
    DECLARE @DialogRecycleMaxMessages BIGINT;

    -- Loop controls.
    DECLARE @messages_per_transaction BIGINT;
    DECLARE @messages_per_dialog_transaction BIGINT;
    DECLARE @transaction_counter INT;
    DECLARE @message_counter BIGINT;

    -- Error variables.
    DECLARE @error_conversation UNIQUEIDENTIFIER;
    DECLARE @error_number INT;
    DECLARE @error_message VARCHAR(4000);
    DECLARE @error_severity INT;
    DECLARE @error_state INT;
    DECLARE @error_procedure SYSNAME;
    DECLARE @error_line INT;
    DECLARE @doomed_transaction SMALLINT;
    DECLARE @error_xml XML;

    -- logging
    DECLARE @TimerDT DATETIME2;
       
    -- Create a table containing requested number of dialogs.
    DECLARE @dialogs TABLE (idx INT, handle UNIQUEIDENTIFIER, recycle_counter BIGINT);
    DECLARE @idx INT;
    DECLARE @handle UNIQUEIDENTIFIER;
    DECLARE @recycle_counter BIGINT;

    -- each row is one message, transform data push parameters to that model
    SET @MessageQuantity = (SELECT MessageQuantity FROM dbo.tTransferParameters);
    SET @RowsToPush = (SELECT RowsToPush FROM dbo.tTransferParameters);
    SET @NumberInitiatorTransactions = (SELECT NumberInitiatorTransactions FROM dbo.tTransferParameters);
    SET @SendTxnWaitTime = (SELECT SendTxnWaitTime FROM dbo.tTransferParameters);
    SET @NumberDialogs = (SELECT NumberDialogs FROM dbo.tTransferParameters);
    SET @DialogRecycleMaxMessages = (SELECT DialogRecycleMaxMessages FROM dbo.tTransferParameters);
    
    -- Compute messages to send per dialog per transaction:
    -- @MessageQuantity / (@NumberInitiatorTransactions x @NumberDialogs)
    -- Note that integer arithmetic may result in "remainder" messages that will not
    -- be sent.
    SET @messages_per_transaction = @MessageQuantity / @NumberInitiatorTransactions;
    SET @messages_per_dialog_transaction = @messages_per_transaction / @NumberDialogs;

    SET @idx = 0;
    WHILE @idx < @NumberDialogs
    BEGIN
         BEGIN DIALOG CONVERSATION @handle
               FROM SERVICE initiator_service
               TO SERVICE 'target_service'
               ON CONTRACT binary_contract
               WITH ENCRYPTION = OFF;
         INSERT INTO @dialogs (idx, handle, recycle_counter) VALUES (@idx, @handle, 0);
         SET @idx = @idx + 1;
    END

    -- Loop through transactions.
    SET @transaction_counter = 0;
    WHILE @transaction_counter < @NumberInitiatorTransactions
    BEGIN
        BEGIN TRANSACTION;
        
        -- Loop through dialogs.
        SET @idx = 0;
        WHILE @idx < @NumberDialogs
        BEGIN
             -- Send a batch of messages for dialog.
             SET @handle = (SELECT handle FROM @dialogs WHERE idx = @idx);
             SET @recycle_counter = (SELECT recycle_counter FROM @dialogs WHERE idx = @idx);
             SET @message_counter = 0;
     
             WHILE @message_counter < @messages_per_dialog_transaction
             BEGIN
                  -- Time to rec`ycle dialog?
                  IF @DialogRecycleMaxMessages <> -1 AND 
                     @recycle_counter = @DialogRecycleMaxMessages
                  BEGIN
                       -- Inform target to end dialog.
                       SEND ON CONVERSATION @handle MESSAGE TYPE end_of_stream;
    
                       -- Replace the current dialog.
                       BEGIN DIALOG CONVERSATION @handle
                           FROM SERVICE initiator_service
                           TO SERVICE 'target_service'
                           ON CONTRACT binary_contract
                           WITH ENCRYPTION = OFF;
                       UPDATE @dialogs SET handle = @handle WHERE idx = @idx;
                       SET @recycle_counter = 0;
                  END
                  
                  -- Send a message.
                  BEGIN TRY
                     
                        IF @LoggingLevel > 1  
                          SET @TimerDT = SYSDATETIME(); 

                        SEND ON CONVERSATION @handle MESSAGE TYPE datagram (@Datagram);

                        IF @LoggingLevel > 1
                          SET @Send_ms = DATEDIFF(ms, @TimerDT, SYSDATETIME()); 

                        IF @DialogRecycleMaxMessages <> -1
                             SET @recycle_counter = @recycle_counter + 1;

                        SET @message_counter = @message_counter + 1;
                 
                  END TRY
                  BEGIN CATCH
                        SET @error_number = ERROR_NUMBER();
                        SET @error_message = ERROR_MESSAGE();

                       -- Dialog is faulty?
                       DECLARE @dialog_error INT;
                       SET @dialog_error = 1;
                       DECLARE @dialog_state VARCHAR(2);
                       SET @dialog_state = (SELECT state FROM sys.conversation_endpoints
                                            WHERE conversation_handle = @handle);
                       IF @@ROWCOUNT = 1
                       BEGIN
                            -- Good dialog is starting or conversing.
                            IF @dialog_state = 'SO' OR @dialog_state = 'CO'
                            BEGIN
                                 SET @dialog_error = 0;
                            END
                       END
                       IF @dialog_error = 1
                       BEGIN
                            -- Record the error.
                            INSERT INTO dbo.initiator_processing_errors 
                              ( [error_conversation]
                              , [error_number]
                              , [error_message]
                              , [error_severity]
                              , [error_state] 
                              , [error_procedure]
                              , [error_line]
                              , [doomed_transaction]
                              , [error_xml] )
                            VALUES
                              ( @handle
                              , @error_number
                              , @error_message
                              , NULL
                              , NULL
                              , NULL
                              , NULL
                              , 0
                              , NULL);

                            -- Replace dialog and continue sending.
                            BEGIN DIALOG CONVERSATION @handle
                                  FROM SERVICE initiator_service
                                  TO SERVICE 'target_service'
                                  ON CONTRACT binary_contract
                                  WITH ENCRYPTION = OFF;
                                  UPDATE @dialogs SET handle = @handle WHERE idx = @idx;
                                  SET @recycle_counter = 0;
                       END
                       ELSE
                       BEGIN
                            -- Record the error and return error.
                            -- Record the error.
                            INSERT INTO dbo.initiator_processing_errors 
                              ( [error_conversation]
                              , [error_number]
                              , [error_message]
                              , [error_severity]
                              , [error_state] 
                              , [error_procedure]
                              , [error_line]
                              , [doomed_transaction]
                              , [error_xml] )
                            VALUES
                              ( @handle
                              , @error_number
                              , @error_message
                              , NULL
                              , NULL
                              , NULL
                              , NULL
                              , 0
                              , NULL);
                            RETURN 1;
                       END
                  END CATCH

             END;

             UPDATE @dialogs SET recycle_counter = @recycle_counter WHERE idx = @idx;
             SET @idx = @idx + 1;
        END

        COMMIT;
        SET @transaction_counter = @transaction_counter + 1;
        
        -- Wait for next transaction.
        IF @transaction_counter < @NumberInitiatorTransactions
        BEGIN

             WAITFOR DELAY @SendTxnWaitTime;

        END
    END

    -- Gracefully end dialogs by informing target.
    BEGIN TRANSACTION;
    SET @idx = 0;
    WHILE @idx < @NumberDialogs
    BEGIN
         SET @handle = (SELECT handle FROM @dialogs WHERE idx = @idx);
         BEGIN
              SEND ON CONVERSATION @handle MESSAGE TYPE end_of_stream;
         END
         SET @idx = @idx + 1;
    END

    COMMIT;

END;

GO

IF OBJECT_ID('dbo.SendXMLPayload', 'P') IS NOT NULL
    DROP PROCEDURE dbo.SendXMLPayload;
GO
-- send one row at a time through service broker.
CREATE PROCEDURE dbo.SendXMLPayload
  ( @Payload XML
  , @LoadMethod NVARCHAR(128)
  , @LoggingLevel TINYINT
  , @Send_ms BIGINT OUTPUT) 
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @TimerDT DATETIME2;
        
    -- Get initiator parameters.
    DECLARE @MessageQuantity BIGINT;
    DECLARE @RowsToPush INT;
    DECLARE @NumberInitiatorTransactions INT;
    DECLARE @SendTxnWaitTime CHAR(12);
    DECLARE @NumberDialogs INT;
    DECLARE @DialogRecycleMaxMessages BIGINT;

    -- Loop controls.
    DECLARE @messages_per_transaction BIGINT;
    DECLARE @messages_per_dialog_transaction BIGINT;
    DECLARE @transaction_counter INT;
    DECLARE @message_counter BIGINT;

    -- Error variables.
    DECLARE @error_conversation UNIQUEIDENTIFIER;
    DECLARE @error_number INT;
    DECLARE @error_message VARCHAR(4000);
    DECLARE @error_severity INT;
    DECLARE @error_state INT;
    DECLARE @error_procedure SYSNAME;
    DECLARE @error_line INT;
    DECLARE @doomed_transaction SMALLINT;
    DECLARE @error_xml XML;
    
    -- Create a table containing requested number of dialogs.
    DECLARE @dialogs TABLE (idx INT, handle UNIQUEIDENTIFIER, recycle_counter BIGINT);
    DECLARE @idx INT;
    DECLARE @handle UNIQUEIDENTIFIER;
    DECLARE @recycle_counter BIGINT;

    SET @Send_ms = 0; 

    -- each row is one message, transform data push parameters to that model
    SET @MessageQuantity = (SELECT MessageQuantity FROM dbo.tTransferParameters);
    SET @RowsToPush = (SELECT RowsToPush FROM dbo.tTransferParameters);
    SET @NumberInitiatorTransactions = (SELECT NumberInitiatorTransactions FROM dbo.tTransferParameters);
    SET @SendTxnWaitTime = (SELECT SendTxnWaitTime FROM dbo.tTransferParameters);
    SET @NumberDialogs = (SELECT NumberDialogs FROM dbo.tTransferParameters);
    SET @DialogRecycleMaxMessages = (SELECT DialogRecycleMaxMessages FROM dbo.tTransferParameters);
    
    -- Compute messages to send per dialog per transaction:
    -- @MessageQuantity / (@NumberInitiatorTransactions x @NumberDialogs)
    -- Note that integer arithmetic may result in "remainder" messages that will not
    -- be sent.
    SET @messages_per_transaction = @MessageQuantity / @NumberInitiatorTransactions;
    SET @messages_per_dialog_transaction = @messages_per_transaction / @NumberDialogs;

    SET @idx = 0;
    WHILE @idx < @NumberDialogs
    BEGIN
         BEGIN DIALOG CONVERSATION @handle
               FROM SERVICE initiator_service
               TO SERVICE 'target_service'
               ON CONTRACT xml_contract
               WITH ENCRYPTION = OFF;
         INSERT INTO @dialogs (idx, handle, recycle_counter) VALUES (@idx, @handle, 0);
         SET @idx = @idx + 1;
    END

    -- Loop through transactions.
    SET @transaction_counter = 0;
    WHILE @transaction_counter < @NumberInitiatorTransactions
    BEGIN
        BEGIN TRANSACTION;
        
        -- Loop through dialogs.
        SET @idx = 0;
        WHILE @idx < @NumberDialogs
        BEGIN
             -- Send a batch of messages for dialog.
             SET @handle = (SELECT handle FROM @dialogs WHERE idx = @idx);
             SET @recycle_counter = (SELECT recycle_counter FROM @dialogs WHERE idx = @idx);
             SET @message_counter = 0;
     
             WHILE @message_counter < @messages_per_dialog_transaction
             BEGIN
                  -- Time to rec`ycle dialog?
                  IF @DialogRecycleMaxMessages <> -1 AND 
                     @recycle_counter = @DialogRecycleMaxMessages
                  BEGIN
                       -- Inform target to end dialog.
                       SEND ON CONVERSATION @handle MESSAGE TYPE end_of_stream;
    
                       -- Replace the current dialog.
                       BEGIN DIALOG CONVERSATION @handle
                           FROM SERVICE initiator_service
                           TO SERVICE 'target_service'
                           ON CONTRACT xml_contract
                           WITH ENCRYPTION = OFF;
                       UPDATE @dialogs SET handle = @handle WHERE idx = @idx;
                       SET @recycle_counter = 0;
                  END
                  
                  -- Send a message.
                  BEGIN TRY

                       IF @LoggingLevel > 1   
                        SET @TimerDT = SYSDATETIME();

                       SEND ON CONVERSATION @handle MESSAGE TYPE payload (@Payload);

                       IF @LoggingLevel > 1
                         SET @Send_ms = DATEDIFF(ms, @TimerDT, SYSDATETIME());


                        IF @DialogRecycleMaxMessages <> -1
                             SET @recycle_counter = @recycle_counter + 1;

                        SET @message_counter = @message_counter + 1;
                 
                  END TRY
                  BEGIN CATCH
                        SET @error_number = ERROR_NUMBER();
                        SET @error_message = ERROR_MESSAGE();

                       -- Dialog is faulty?
                       DECLARE @dialog_error INT;
                       SET @dialog_error = 1;
                       DECLARE @dialog_state VARCHAR(2);
                       SET @dialog_state = (SELECT state FROM sys.conversation_endpoints
                                            WHERE conversation_handle = @handle);
                       IF @@ROWCOUNT = 1
                       BEGIN
                            -- Good dialog is starting or conversing.
                            IF @dialog_state = 'SO' OR @dialog_state = 'CO'
                            BEGIN
                                 SET @dialog_error = 0;
                            END
                       END
                       IF @dialog_error = 1
                       BEGIN
                            -- Record the error.
                            INSERT INTO dbo.initiator_processing_errors 
                              ( [error_conversation]
                              , [error_number]
                              , [error_message]
                              , [error_severity]
                              , [error_state] 
                              , [error_procedure]
                              , [error_line]
                              , [doomed_transaction]
                              , [error_xml] )
                            VALUES
                              ( @handle
                              , @error_number
                              , @error_message
                              , NULL
                              , NULL
                              , NULL
                              , NULL
                              , 0
                              , NULL);

                            -- Replace dialog and continue sending.
                            BEGIN DIALOG CONVERSATION @handle
                                  FROM SERVICE initiator_service
                                  TO SERVICE 'target_service'
                                  ON CONTRACT xml_contract
                                  WITH ENCRYPTION = OFF;
                                  UPDATE @dialogs SET handle = @handle WHERE idx = @idx;
                                  SET @recycle_counter = 0;
                       END
                       ELSE
                       BEGIN
                            -- Record the error and return error.
                            -- Record the error.
                            INSERT INTO dbo.initiator_processing_errors 
                              ( [error_conversation]
                              , [error_number]
                              , [error_message]
                              , [error_severity]
                              , [error_state] 
                              , [error_procedure]
                              , [error_line]
                              , [doomed_transaction]
                              , [error_xml] )
                            VALUES
                              ( @handle
                              , @error_number
                              , @error_message
                              , NULL
                              , NULL
                              , NULL
                              , NULL
                              , 0
                              , NULL);
                            RETURN 1;
                       END
                  END CATCH

             END;

             UPDATE @dialogs SET recycle_counter = @recycle_counter WHERE idx = @idx;
             SET @idx = @idx + 1;
        END

        COMMIT;
        SET @transaction_counter = @transaction_counter + 1;
        
        -- Wait for next transaction.
        IF @transaction_counter < @NumberInitiatorTransactions
        BEGIN

             WAITFOR DELAY @SendTxnWaitTime;

        END
    END

    -- Gracefully end dialogs by informing target.
    BEGIN TRANSACTION;
    SET @idx = 0;
    WHILE @idx < @NumberDialogs
    BEGIN
         SET @handle = (SELECT handle FROM @dialogs WHERE idx = @idx);
         BEGIN
              SEND ON CONVERSATION @handle MESSAGE TYPE end_of_stream;
         END
         SET @idx = @idx + 1;
    END

    COMMIT;
                
END;

GO

IF OBJECT_ID('dbo.ResendPendingXML', 'P') IS NOT NULL
    DROP PROCEDURE dbo.ResendPendingXML 

GO

-- Resends all pending messages in sys.transmission_queue
-- belonging to an old conversation on a new conversation.
CREATE PROCEDURE dbo.ResendPendingXML (@old_handle UNIQUEIDENTIFIER)
AS
BEGIN
     SET NOCOUNT ON;

     DECLARE @message_type_name SYSNAME;
     DECLARE @message_body VARBINARY(MAX);
     
     -- Get a new dialog.
     DECLARE @handle UNIQUEIDENTIFIER;
     BEGIN DIALOG CONVERSATION @handle
           FROM SERVICE initiator_service
           TO SERVICE 'target_service'
           ON CONTRACT xml_contract
           WITH ENCRYPTION = OFF;

     -- Declare a cursor to iterate over all the pending messages.
     -- It is important to keep the message order and to keep the original message type.
     DECLARE cursor_pending CURSOR LOCAL FORWARD_ONLY READ_ONLY
            FOR SELECT message_type_name, message_body
            FROM sys.transmission_queue
            WHERE conversation_handle = @old_handle
            ORDER BY message_sequence_number;
     OPEN cursorPending;

     FETCH NEXT FROM cursor_pending INTO @message_type_name, @message_body;
     WHILE (@@FETCH_STATUS = 0)
     BEGIN
          -- Resend the message on the new conversation
          SEND ON CONVERSATION @handle MESSAGE TYPE @message_type_name (@message_body);

          FETCH NEXT FROM cursor_pending INTO @message_type_name, @message_body;
     END
     CLOSE cursor_pending;
     DEALLOCATE cursor_pending;
     
     -- Signal end of stream to target.
     SEND ON CONVERSATION @handle MESSAGE TYPE end_of_stream;

END;
GO

IF OBJECT_ID('dbo.ResendPendingBinary', 'P') IS NOT NULL
    DROP PROCEDURE dbo.ResendPendingBinary 

GO

-- Resends all pending messages in sys.transmission_queue
-- belonging to an old conversation on a new conversation.
CREATE PROCEDURE dbo.ResendPendingBinary (@old_handle UNIQUEIDENTIFIER)
AS
BEGIN
     SET NOCOUNT ON;

     DECLARE @message_type_name SYSNAME;
     DECLARE @message_body VARBINARY(MAX);
     
     -- Get a new dialog.
     DECLARE @handle UNIQUEIDENTIFIER;
     BEGIN DIALOG CONVERSATION @handle
           FROM SERVICE initiator_service
           TO SERVICE 'target_service'
           ON CONTRACT binary_contract
           WITH ENCRYPTION = OFF;

     -- Declare a cursor to iterate over all the pending messages.
     -- It is important to keep the message order and to keep the original message type.
     DECLARE cursor_pending CURSOR LOCAL FORWARD_ONLY READ_ONLY
            FOR SELECT message_type_name, message_body
            FROM sys.transmission_queue
            WHERE conversation_handle = @old_handle
            ORDER BY message_sequence_number;
     OPEN cursorPending;

     FETCH NEXT FROM cursor_pending INTO @message_type_name, @message_body;
     WHILE (@@FETCH_STATUS = 0)
     BEGIN
          -- Resend the message on the new conversation
          SEND ON CONVERSATION @handle MESSAGE TYPE @message_type_name (@message_body);

          FETCH NEXT FROM cursor_pending INTO @message_type_name, @message_body;
     END
     CLOSE cursor_pending;
     DEALLOCATE cursor_pending;
     
     -- Signal end of stream to target.
     SEND ON CONVERSATION @handle MESSAGE TYPE end_of_stream;

END;
GO

IF OBJECT_ID('dbo.pInitiatorActivationProcedure', 'P') IS NOT NULL
    DROP PROCEDURE dbo.pInitiatorActivationProcedure
GO
-- Activated store proc for the initiator to receive messages.
-- Dialogs are gracefully ended by the target after receiving
-- an end_of_stream message from the initiator; the end dialog
-- message is then processed here. This method is recommended
-- to avoid "fire and forget" message loss. One message per
-- invocation is OK here for expected low-volume load.
CREATE PROCEDURE dbo.pInitiatorActivationProcedure
AS
BEGIN
     SET NOCOUNT ON;
       
     DECLARE @conversation_handle UNIQUEIDENTIFIER
           , @service_contract_name NVARCHAR(256)
           , @message_type_name SYSNAME
           , @message_body VARBINARY(MAX);

     -- Error variables.
     DECLARE @error_conversation UNIQUEIDENTIFIER;
     DECLARE @error_number INT;
     DECLARE @error_message VARCHAR(4000);
     DECLARE @error_severity INT;
     DECLARE @error_state INT;
     DECLARE @error_procedure SYSNAME;
     DECLARE @error_line INT;
     DECLARE @doomed_transaction SMALLINT;
     DECLARE @error_xml XML;

     BEGIN TRY
     BEGIN TRANSACTION;

     -- Wait 5 seconds for a message.
     WAITFOR (
          RECEIVE TOP(1)
                  @conversation_handle = conversation_handle
                , @service_contract_name = service_contract_name
                , @message_type_name = message_type_name
                , @message_body = message_body
          FROM initiator_queue), TIMEOUT 5000;

     IF @@ROWCOUNT = 1
     BEGIN
          IF @message_type_name = 'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog'
          BEGIN
               -- Target is ending dialog normally.
               END CONVERSATION @conversation_handle;
          END
          ELSE IF @message_type_name = 'http://schemas.microsoft.com/SQL/ServiceBroker/Error'
          BEGIN
               -- Record the error.
               WITH XMLNAMESPACES ('http://schemas.microsoft.com/SQL/ServiceBroker/Error' AS ssb)
               SELECT
               @error_number = CAST(@message_body AS XML).value('(//ssb:Error/ssb:Code)[1]', 'INT'),
               @error_message = CAST(@message_body AS XML).value('(//ssb:Error/ssb:Description)[1]', 'VARCHAR(4000)');
               INSERT INTO dbo.initiator_processing_errors
                 ( [error_conversation]
                 , [error_number]
                 , [error_message]
                 , [error_severity]
                 , [error_state] 
                 , [error_procedure]
                 , [error_line]
                 , [doomed_transaction]
                 , [error_xml] )
               VALUES( @conversation_handle
                     , @error_number
                     , @error_message
                     , NULL
                     , NULL
                     , OBJECT_NAME(@@PROCID)
                     , NULL
                     , 0 -- we are here so must not have been fatal
                     , CAST(@message_body AS XML));
               -- Can messages be resent?
               IF (@error_number IN (-8489, -8462, -9719, -28052))
               BEGIN
                    -- Resend the messages on a new dialog.
                    IF @service_contract_name = 'binary_contract'
                      EXEC ResendPendingBinary @conversation_handle;                      

                    ELSE --hopefully @service_contract_name = 'xml_contract'
                      EXEC ResendPendingXML @conversation_handle;

               END
               ELSE
               BEGIN
                    -- Save the messages in a side table to be processed later.
                    INSERT INTO dbo.unsent_messages
                     ( message_type_name
                     , message_body) 
                    SELECT message_type_name
                         , message_body 
                    FROM sys.transmission_queue
                    WHERE conversation_handle = @conversation_handle;
               END

               -- End the conversation.
               END CONVERSATION @conversation_handle;
          END
     END
     COMMIT;
     END TRY
     BEGIN CATCH
           SET @error_conversation = @conversation_handle
           SET @error_number = ISNULL(ERROR_NUMBER(), '?');
           SET @error_message = ISNULL(ERROR_MESSAGE(), '?');
           SET @error_severity = ISNULL(ERROR_SEVERITY(), '?');
           SET @error_state = ISNULL(ERROR_STATE(), '?');
           SET @error_procedure = ISNULL(ERROR_PROCEDURE(), '?');
           SET @error_line = ISNULL(ERROR_LINE(), '?');
           SET @doomed_transaction = XACT_STATE();
           SET @error_xml = EVENTDATA();

           IF @doomed_transaction = -1
           BEGIN
                -- Note: 5 consecutive rollbacks will disable the queue!
                ROLLBACK TRANSACTION;
                BEGIN TRANSACTION; 
           END
            -- Record error and commit transaction.
           INSERT INTO dbo.initiator_processing_errors
              ( [error_conversation]
              , [error_number]
              , [error_message]
              , [error_severity]
              , [error_state] 
              , [error_procedure]
              , [error_line]
              , [doomed_transaction]
              , [error_xml] )
            VALUES( @conversation_handle
                  , ERROR_NUMBER()
                  , ERROR_MESSAGE()
                  , ERROR_SEVERITY()
                  , ERROR_STATE()
                  , ERROR_PROCEDURE()
                  , ERROR_LINE()
                  , XACT_STATE()
                  , NULL );
           COMMIT;
     END CATCH
END;
GO

-- Drop & Create Service Broker objects in dependency order

USE msdb;
GO
IF EXISTS (SELECT * FROM sys.routes WHERE name = 'initiator_route')
    DROP ROUTE initiator_route;

GO
USE $(INITIATORDB);
GO        

IF EXISTS (SELECT * FROM sys.routes WHERE name = 'target_route')
    DROP ROUTE target_route;

GO

IF EXISTS (SELECT * FROM sys.services WHERE name = 'initiator_service')
    DROP SERVICE initiator_service;

GO        
IF EXISTS (SELECT * FROM sys.services WHERE name = 'target_service')
    DROP SERVICE target_service;
GO        
IF EXISTS (SELECT * FROM sys.service_queues WHERE name = 'initiator_queue')
    DROP QUEUE initiator_queue;
GO
If EXISTS (SELECT * FROM sys.service_contracts where name = 'xml_contract')
 DROP CONTRACT xml_contract;
GO
If EXISTS (SELECT * FROM sys.service_contracts where name = 'binary_contract')
 DROP CONTRACT binary_contract;
GO
IF EXISTS (SELECT * FROM sys.service_message_types WHERE name = 'payload')
 DROP MESSAGE TYPE payload;
GO
IF EXISTS (SELECT * FROM sys.service_message_types WHERE name = 'datagram')
 DROP MESSAGE TYPE datagram;
GO
IF EXISTS (SELECT * FROM sys.service_message_types WHERE name = 'end_of_stream')
 DROP MESSAGE TYPE end_of_stream;
GO
IF EXISTS (SELECT * FROM sys.database_principals WHERE name = 'BrokerSenderRole')
 DROP ROLE BrokerSenderRole;
GO
CREATE ROLE BrokerSenderRole;
GO
CREATE MESSAGE TYPE payload VALIDATION = NONE;
GO
CREATE MESSAGE TYPE datagram VALIDATION = NONE;
GO
CREATE MESSAGE TYPE end_of_stream;
GO
CREATE CONTRACT xml_contract
       (
        payload SENT BY INITIATOR,
        end_of_stream SENT BY INITIATOR
       );
GO
CREATE CONTRACT binary_contract
       (
        datagram SENT BY INITIATOR,
        end_of_stream SENT BY INITIATOR
       );
GO

---------------------------
--  Initiator Specific
--------------------------- 
BEGIN
 -- Create the initiator queue with activated procedure.
 CREATE QUEUE dbo.initiator_queue
 WITH ACTIVATION (
       STATUS = ON,
       MAX_QUEUE_READERS = 1,
       PROCEDURE_NAME = [dbo].[pInitiatorActivationProcedure],
       EXECUTE AS OWNER);

 IF @@ERROR <> 0
  -- level 10 returns only message line no error,state,level
  -- the actual error will be displayed above this message
  RAISERROR('Not all initiator side Service Broker objects have been created.',10,1)
 ElSE
  BEGIN
   -- Create initiator service.
   CREATE SERVICE initiator_service
   AUTHORIZATION dbo 
   ON QUEUE dbo.initiator_queue ( binary_contract
                                , xml_contract);

   -- Any valid user in the database can send messages on this service.
   GRANT SEND ON SERVICE::initiator_service TO BrokerSenderRole;
  END;

END;

-----------------------------------------------------------------------------
-- even though we are working on the initiator
-- we tried to drop target_queue above to remove the dependency so
-- recreate target service IF the target queue exists in the local db  
-- will only be true if initiator and target are on same SQL Instance
----------------------------------------------------------------------------- 
IF EXISTS (SELECT * FROM sys.service_queues WHERE name = 'target_queue')
 BEGIN

   -- Create initiator service.
   CREATE SERVICE target_service 
   AUTHORIZATION dbo
   ON QUEUE target_queue ( binary_contract
                         , xml_contract);

   -- Any valid user in the database can send messages on this service.
   GRANT SEND ON SERVICE::target_service TO BrokerSenderRole;
 END;

GO

-- Create a route to the target service.
IF DB_NAME() = '$(INITIATORDB)'
 CREATE ROUTE target_route
	WITH SERVICE_NAME = 'target_service',
	ADDRESS = 'tcp://$(TARGETHOST):$(TARGETPORT)';

GO

-- In msdb, create an incoming route to the initiator service.
USE msdb;
GO
IF DB_ID('$(INITIATORDB)') IS NOT NULL
  CREATE ROUTE initiator_route
	 WITH SERVICE_NAME = 'initiator_service',
	 ADDRESS = 'local';
GO

----------------------------------------------------
-- Create the broker endpoint using Windows authentication.
-- On a secure network, encryption may be disabled to improve speed:
-- (AUTHENTICATION = Windows, ENCRYPTION = DISABLED)
--
-- Create a login for the partner machine (partner_host) in the
-- shared domain (domain_name) and grant it endpoint connection permission.
-- This assumes the availability of Kerberos authentication.
-- Note: the '$' is significant.
-- 
-- give the login permission to use the endpoint
---------------------------------------------------------------------

USE master;

GO

IF NOT EXISTS (SELECT * FROM sys.endpoints 
               WHERE type_desc = 'SERVICE_BROKER'
               AND protocol_desc = 'TCP')
AND DB_ID('$(TARGETDB)') IS NULL 
AND DB_ID('$(INITIATORDB)') IS NOT NULL    
  CREATE ENDPOINT ServiceBrokerEndpoint
  STATE = STARTED
  AS TCP (LISTENER_PORT = $(INITIATORPORT))
  FOR SERVICE_BROKER (AUTHENTICATION = Windows, ENCRYPTION = $(ENCRYPTION));

GO

-- requires Kerboros for the "domain_name\machine_name$" voodoo to work

-- ?? only needed and can only  be used if initiator and target not on same SQL instance
/*
DECLARE @DomainName NVARCHAR(128)
      , @InitiatorHost NVARCHAR(128)
      , @TargetHost NVARCHAR(128)
      , @InitiatorLogin NVARCHAR(256)
      , @TargetLogin NVARCHAR(256);
SET @DomainName = '$(DOMAINNAME)';
SET @TargetHost = '$(TARGETHOST)';
SET @InitiatorHost = '$(INITIATORHOST)';
                          
IF DB_ID('$(INITIATORDB)') IS NOT NULL
AND SERVERPROPERTY('MachineName') = '$(INITIATORHOST)'
AND SERVERPROPERTY('MachineName') <> '$(TARGETHOST)' 
  BEGIN
    SET @InitiatorLogin = @DomainName 
                        + CASE WHEN @DomainName = '' 
                               THEN ''
                               ELSE '\'
                               END
                         + @TargetHost 
                         + '$';
    IF NOT EXISTS (SELECT * FROM sys.syslogins WHERE name = @InitiatorLogin)
      EXEC('CREATE LOGIN ' + @InitiatorLogin + ' FROM Windows');
    EXEC('GRANT CONNECT ON ENDPOINT::ServiceBrokerEndpoint TO ' + @InitiatorLogin);
  END  

EXEC ('CREATE REMOTE SERVICE BINDING APBinding TO SERVICE ''target_service'' WITH USER = ''' 
     + @InitiatorLogin + ''', ANONYMOUS = OFF');
*/
GO

----------------------------------------------------------------
-- hold over exception logging tables from fastdataload sample
----------------------------------------------------------------

-- This table stores unsent messages.
IF EXISTS (SELECT name FROM sys.tables WHERE name = 'unsent_messages')
	DROP TABLE dbo.unsent_messages;
GO

CREATE TABLE dbo.unsent_messages 
 ( Id INT IDENTITY(1,1)
 , message_type_name SYSNAME
 , message_body VARCHAR(MAX)
 , CreateDt DATETIME2
   CONSTRAINT dft_unsent_messages__CreateDT
   DEFAULT (SYSDATETIME()))
 WITH (DATA_COMPRESSION = ROW);
GO

-- Table to store processing errors.
IF EXISTS (SELECT name FROM sys.tables WHERE name = 'initiator_processing_errors')
	DROP TABLE dbo.initiator_processing_errors;
GO

CREATE TABLE dbo.initiator_processing_errors 
  ( [Id] INT IDENTITY(1,1)
  , [error_conversation] UNIQUEIDENTIFIER
  , [error_number] INT
  , [error_message] VARCHAR(4000)
  , [error_severity] INT
  , [error_state] INT
  , [error_procedure] SYSNAME NULL
  , [error_line] INT
  , [doomed_transaction] TINYINT
  , [error_xml] XML 
  , CreateDt DATETIME2
    CONSTRAINT dft_initiator_processing_errors__CreateDT
    DEFAULT (SYSDATETIME())
  , CONSTRAINT pk_initiator_processing_errors 
    PRIMARY KEY (Id))
WITH (DATA_COMPRESSION = ROW);

GO

USE master;
