USE [target]
GO

/****** Object:  UserDefinedFunction [dbo].[fBinaryMarshalTVP]    Script Date: 11/12/2010 05:58:43 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[fBinaryMarshalTVP]
  ( @TableValuedParameter TVPTableType READONLY ) 
RETURNS VARBINARY(MAX)
AS
BEGIN
  DECLARE @bigbin VARBINARY(MAX)
        , @RowCount BIGINT
        , @TableVariable TVPTableType;

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
  FROM @TableValuedParameter;
  
  SET @RowCount = @@ROWCOUNT;
  SET @bigbin = CAST(CAST('TVP' AS CHAR(3)) AS VARBINARY(MAX))  
              + CAST(@RowCount AS VARBINARY(MAX));

  SELECT @bigbin = @bigbin 
                 + CAST(DATALENGTH(CAST(row AS VARBINARY(MAX))) AS BINARY(8)) 
                 + CAST(row AS VARBINARY(MAX))
  FROM (SELECT CAST(RowNumber AS BINARY(4))
             + CAST(CAST(DATALENGTH(CAST(Host AS VARBINARY(20))) AS TINYINT) AS BINARY(1))
             + CAST(Host AS VARBINARY(20))
             + CAST(GMTDateTime AS BINARY(8))
             + CAST(CAST(DATALENGTH(CAST(Name AS VARBINARY(MAX))) AS TINYINT) AS BINARY(1))
             + CAST(Name AS VARBINARY(40))
             + CAST(Value AS BINARY(4))                                                                 
             + CAST(STR(FloatingPoint, 20, 16) AS BINARY(20))  
             + CAST(Boolean AS BINARY(1))
             + CAST(CAST(DATALENGTH(CAST(Bytes AS VARBINARY(MAX))) AS TINYINT) AS BINARY(1))            
             + Bytes                                                                 
             + CAST(CAST(DATALENGTH(CAST(LOB AS VARBINARY(MAX))) AS INT) AS BINARY(4))                  
             + CAST(LOB AS VARBINARY(MAX))                                                                   
             + CAST(CAST(DATALENGTH(LOB) AS INT) AS BINARY(4)) as [row]
        FROM @TableValuedParameter) derived

  RETURN @bigbin;
  
END;


GO

USE [target]
GO

/****** Object:  StoredProcedure [dbo].[pSQLTVPAsyncMerge]    Script Date: 11/12/2010 05:57:49 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- The data push procedure: send messages to initiator.
CREATE PROCEDURE [dbo].[pSQLTVPAsyncMerge]
    ( @TVP TVPTableType READONLY
    , @LoggingLevel TINYINT = 0)
AS
BEGIN
    SET NOCOUNT ON;

    -- Get initiator parameters.
    DECLARE @message_quantity BIGINT;
    DECLARE @rows_per_message INT;
    DECLARE @number_initiator_transactions INT;
    DECLARE @initiator_transaction_delay CHAR(12);
    DECLARE @number_dialogs INT;
    DECLARE @dialog_recycle_max_messages BIGINT;

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

    -- variables for moving set into queue
    DECLARE @TableVariable TVPTableType;
    DECLARE @payload XML;
    
    SET @message_quantity = (SELECT message_quantity FROM data_push_parameters);
    SET @rows_per_message = (SELECT rows_per_message FROM data_push_parameters);
    SET @number_initiator_transactions = (SELECT number_initiator_transactions FROM data_push_parameters);
    SET @initiator_transaction_delay = (SELECT initiator_transaction_delay FROM data_push_parameters);
    SET @number_dialogs = (SELECT number_dialogs FROM data_push_parameters);
    SET @dialog_recycle_max_messages = (SELECT dialog_recycle_max_messages FROM data_push_parameters);
    SET @LoggingLevel = (SELECT logging_level FROM data_push_parameters);

    -- Compute messages to send per dialog per transaction:
    -- @message_quantity / (@number_initiator_transactions x @number_dialogs)
    -- Note that integer arithmetic may result in "remainder" messages that will not
    -- be sent.
    SET @messages_per_transaction = @message_quantity / @number_initiator_transactions;
    SET @messages_per_dialog_transaction = @messages_per_transaction / @number_dialogs;

    -- Create a table containing requested number of dialogs.
    DECLARE @dialogs TABLE (idx INT, handle UNIQUEIDENTIFIER, recycle_counter BIGINT);
    DECLARE @idx INT;
    DECLARE @handle UNIQUEIDENTIFIER;
    DECLARE @recycle_counter BIGINT;

    IF @LoggingLevel > 1
     BEGIN
         -- for elapsed time tracking but will degrad peformance in direct correlation to Load granularity
         -- does not participate in transaction, just need to make sure it always gets dumped at the end
         DECLARE @ActivityLog TABLE ( [Id] INT IDENTITY(1,1) PRIMARY KEY
                                    , [Action] NVARCHAR(30)
                                    , [Value] FLOAT DEFAULT (@@ROWCOUNT)
                                    , [DateStamp] DATETIME2 DEFAULT (SYSDATETIME())); 


         INSERT @ActivityLog ([Action])
         VALUES ('TransmissionStart');
     END

    SET @idx = 0;
    WHILE @idx < @number_dialogs
    BEGIN
         BEGIN DIALOG CONVERSATION @handle
               FROM SERVICE initiator_service
               TO SERVICE 'target_service'
               ON CONTRACT data_push_contract
               WITH ENCRYPTION = OFF;
         INSERT INTO @dialogs (idx, handle, recycle_counter) VALUES (@idx, @handle, 0);
         SET @idx = @idx + 1;
    END

    -- Loop through transactions.
    SET @transaction_counter = 0;
    WHILE @transaction_counter < @number_initiator_transactions
    BEGIN
        BEGIN TRANSACTION;
        
        -- Loop through dialogs.
        SET @idx = 0;
        WHILE @idx < @number_dialogs
        BEGIN
             -- Send a batch of messages for dialog.
             SET @handle = (SELECT handle FROM @dialogs WHERE idx = @idx);
             SET @recycle_counter = (SELECT recycle_counter FROM @dialogs WHERE idx = @idx);
             SET @message_counter = 0;
             WHILE @message_counter < @messages_per_dialog_transaction
             BEGIN
                  -- Time to recycle dialog?
                  IF @dialog_recycle_max_messages <> -1 AND 
                     @recycle_counter = @dialog_recycle_max_messages
                  BEGIN
                       -- Inform target to end dialog.
                       SEND ON CONVERSATION @handle MESSAGE TYPE end_of_stream;
    
                       -- Replace the current dialog.
                       BEGIN DIALOG CONVERSATION @handle
                           FROM SERVICE initiator_service
                           TO SERVICE 'target_service'
                           ON CONTRACT data_push_contract
                           WITH ENCRYPTION = OFF;
                       UPDATE @dialogs SET handle = @handle WHERE idx = @idx;
                       SET @recycle_counter = 0;
                  END
                  
                  -- Send a message.
                  BEGIN TRY
                        BEGIN
                             IF @LoggingLevel > 2
                              INSERT @ActivityLog ([Action])
                              VALUES ('DeleteTableVarStart');
                            
                             DELETE @TableVariable;

                             IF @LoggingLevel > 2
                               BEGIN   
                                INSERT @ActivityLog ([Action])
                                VALUES ('DeleteTableVarComplete');

                                INSERT @ActivityLog ([Action])
                                VALUES ('TVPtoTableVarStart');
                               END;

                             -- send the rows to initiator queue as XML
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
                             FROM @TVP;
                                   
                             IF @LoggingLevel > 2
                              BEGIN
                               INSERT @ActivityLog ([Action])
                               VALUES ('TVPtoTableVarComplete');

                               INSERT @ActivityLog ([Action])
                               VALUES ('TableVartoXMLStart');
                              END;  

                             -- binary data needs to go to XML as a string
                             SET @Payload = '<rows>'
                                          + (SELECT RowNumber
                                                  , Host
                                                  , GMTDateTime
                                                  , Name
                                                  , Value
                                                  , FloatingPoint
                                                  , Boolean
                                                  , CONVERT(VARCHAR(MAX),Bytes,1) AS [Bytes]
                                                  , LOB 
                                                  , LOBCheckSum 
                                             FROM @TableVariable 
                                             AS [row] FOR XML AUTO)
                                           + '</rows>';
                                                 
                             IF @LoggingLevel > 2
                              INSERT @ActivityLog ([Action])
                              VALUES ('TableVartoXMLComplete');

                             IF @LoggingLevel > 2
                              INSERT @ActivityLog ([Action])
                              VALUES ('SendStart');

                             SEND ON CONVERSATION @handle MESSAGE TYPE data_push_message (@Payload);

                             IF @LoggingLevel > 2
                              INSERT @ActivityLog ([Action])
                              VALUES ('SendComplete');

                        END

                        IF @dialog_recycle_max_messages <> -1
                        BEGIN
                             SET @recycle_counter = @recycle_counter + 1;
                        END
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
                                  ON CONTRACT data_push_contract
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
             END
             UPDATE @dialogs SET recycle_counter = @recycle_counter WHERE idx = @idx;
             SET @idx = @idx + 1;
        END

        COMMIT;
        SET @transaction_counter = @transaction_counter + 1;
        
        -- Wait for next transaction.
        IF @transaction_counter < @number_initiator_transactions
        BEGIN

            IF @LoggingLevel > 1
             INSERT @ActivityLog ([Action])
             VALUES ('BuiltinDelayStart');

             WAITFOR DELAY @initiator_transaction_delay;

            IF @LoggingLevel > 1
             INSERT @ActivityLog ([Action])
             VALUES ('BuiltinDelayComplete');

        END
    END

    -- Gracefully end dialogs by informing target.
    BEGIN TRANSACTION;
    SET @idx = 0;
    WHILE @idx < @number_dialogs
    BEGIN
         SET @handle = (SELECT handle FROM @dialogs WHERE idx = @idx);
         BEGIN
              SEND ON CONVERSATION @handle MESSAGE TYPE end_of_stream;
         END
         SET @idx = @idx + 1;
    END

    COMMIT;
    
    IF @LoggingLevel > 1
     BEGIN
      INSERT @ActivityLog (Action)
      SELECT 'TransmissionComplete';

      -- log the receive for performance tracking  
      BEGIN TRANSACTION;
      INSERT dbo.ActivityLog 
        ( LoadMethod
        , Action
        , DateStamp
        , Spid)
      SELECT OBJECT_NAME(@@PROCID)
           , Action
           , DateStamp
           , @@SPID 
      FROM @ActivityLog; 
      COMMIT;
     END;
            
    RETURN 0;
END;


GO

/****** Object:  StoredProcedure [dbo].[pSQLTVPSyncMerge]    Script Date: 11/12/2010 05:57:50 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--------------------------------------------------------------------
-- all other SQL load methods call dbo.pSQLTVPSyncMerge
--   pSQLRowSyncMerge -> dbo.pSQLTVPSyncMerge
--   pSQLRowAsyncMerge -> Initiator -> Target -> dbo.pSQLTVPSyncMerge
--   pSQLTVPAsyncMerge -> Initiator -> Target -> dbo.pSQLTVPSyncMerge
-- the File load methods use the same MERGE but no table variable (?)
---------------------------------------------------------------------
CREATE PROCEDURE [dbo].[pSQLTVPSyncMerge]
    ( @TVP TVPTableType READONLY 
    , @LoggingLevel TINYINT)
AS
BEGIN
 DECLARE @MergeMapper TABLE 
     ( Id BIGINT
     , Name VARCHAR(50)
     , LOBCheckSum INT
     , GMTDate DATE
     , GMTHour INT);

 DECLARE @TableVariable TVPTableType;

 IF @LoggingLevel > 1
  INSERT target.dbo.ActivityLog ([Action])
  VALUES ('TVPToTableVarStart');

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
 FROM @TVP;

 IF @LoggingLevel > 1
  INSERT target.dbo.ActivityLog ([Action])
  VALUES ('TVPToTableVarComplete');
 
 IF @LoggingLevel > 1
  INSERT target.dbo.ActivityLog ([Action])
  VALUES ('MergeStart');

 MERGE dbo.Parent AS p
 USING (SELECT aggon.RowNumber as RowNumber
             , tv.[Name]
             , CAST(tv.[GMTDateTime] AS DATE) AS [GMTDate]
             , DATEPART(hour, tv.[GMTDateTime]) AS [GMTHour]
             , tv.[LOB]
             , tv.[LOBCheckSum]
             , aggon.[Occurrences]
        FROM @TableVariable tv
        JOIN (SELECT RowNumber
                   , Occurrences
              FROM (SELECT [Name]
                         , [LOBCheckSum]
                         , CAST([GMTDateTime] AS DATE) AS [GMTDate] 
                         , DATEPART(hour, [GMTDateTime]) AS [GMTHour]
                         , MIN(RowNumber) AS [RowNumber]
                         , COUNT(*) AS [Occurrences]
                    FROM @TableVariable
                    GROUP BY [Name]
                           , [LOBCheckSum]
                           , CAST([GMTDateTime] AS DATE)
                           , DATEPART(hour, [GMTDateTime])) AS [first]) AS [aggon]
        ON tv.[RowNumber] = aggon.[RowNumber]) AS t
 ON p.[Name] = t.[Name]
 AND p.[LOBCheckSum] = t.[LOBCheckSum]
 AND p.[GMTDate] = t.[GMTDate]
 AND p.[GMTHour] = t.[GMTHour]
 WHEN MATCHED THEN
     UPDATE SET [Occurrences] = p.[Occurrences] + 1
              , [LastUpdateDate] = CAST(SYSDATETIME() AS DATE)
              , [LastUpdateTime] = CAST(SYSDATETIME() AS TIME)     
 WHEN NOT MATCHED THEN
     INSERT ( [GMTDate] 
            , [GMTHour] 
            , [Name]
            , [LOBCheckSum]
            , [LOB] )
      VALUES
            ( t.[GMTDate]
            , t.[GMTHour]
            , t.[Name]
            , t.[LOBCheckSum]
            , t.[LOB])
 OUTPUT Inserted.Id, Inserted.Name, Inserted.LOBCheckSum, Inserted.GMTDate, Inserted.GMTHour INTO @MergeMapper;

 IF @LoggingLevel > 1
  INSERT target.dbo.ActivityLog ([Action])
  VALUES ('MergeComplete');

 IF @LoggingLevel > 1
  INSERT target.dbo.ActivityLog ([Action])
  VALUES ('ChildInsertStart');

 INSERT dbo.Child 
  ( ParentId 
  , GMTDateTime
  , Value
  , Host
  , FloatingPoint
  , Boolean
  , Bytes )
 SELECT 
    p.Id
  , d.GMTDateTime
  , d.Value
  , d.Host
  , d.FloatingPoint
  , d.Boolean
  , d.Bytes
 FROM @MergeMapper p
 JOIN @TableVariable d
 ON p.[Name] = d.[Name]
 AND p.[LOBCheckSum] = d.[LOBCheckSum]
 AND p.[GMTDate] = CAST(d.GMTDateTime AS DATE)
 AND p.[GMTHour] = DATEPART(hour, d.GMTDateTime); 

 IF @LoggingLevel > 1
  INSERT target.dbo.ActivityLog ([Action])
  VALUES ('ChildInsertComplete');

END;

GO

/****** Object:  StoredProcedure [dbo].[StageUpsert]    Script Date: 11/12/2010 05:57:51 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

------------------------------------------
-- unload a TVP into a temp table that can
-- be indexed after load for faster merge
------------------------------------------
-- if it fails how will this new table or 
-- the sent data ever get found again? 
-- Maybe use failed_messages table
-- from the fast_data_load sample?
------------------------------------------
CREATE PROCEDURE [dbo].[StageUpsert] 
 ( @TVP TVPTableType READONLY 
 , @LoggingLevel TINYINT
 , @LoadMethod NVARCHAR(128) 
 , @TVPToTempTableRows BIGINT OUTPUT 
 , @TVPToTempTable_ms BIGINT OUTPUT
 , @CreateIndex_ms BIGINT OUTPUT 
 , @MergeParentRows BIGINT OUTPUT 
 , @MergeParent_ms BIGINT OUTPUT
 , @InsertChildRows BIGINT OUTPUT 
 , @InsertChild_ms BIGINT OUTPUT )
AS
BEGIN

 DECLARE @TimerDT DATETIME2; 

 DECLARE @MergeMapper TABLE 
   ( Id BIGINT
   , Name VARCHAR(50)
   , LOBCheckSum INT
   , GMTDate DATE
   , GMTHour INT);

 IF @LoggingLevel > 1
   SET @TimerDT = SYSDATETIME(); 

 -- stage table is a local temp table, for this reason this will 
 -- not work if the merge needs to be asynchronous from the import 
 CREATE TABLE #SourceData
    ( RowNumber INT
    , Host NVARCHAR(128)
    , GMTDateTime DATETIME
    , Name VARCHAR(50)
    , Value INT 
    , FloatingPoint REAL
    , Boolean BIT
    , Bytes VARBINARY(60)
    , LOB NVARCHAR(MAX)
    , LOBCheckSum INT);

 INSERT #SourceData
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
 FROM  @TVP;
 SET @TVPToTempTable_ms = @@ROWCOUNT;
                 
 IF @LoggingLevel > 1
   SET @TVPToTempTable_ms = DATEDIFF(ms, @TimerDt, SYSDATETIME());

 IF @LoggingLevel > 1
   SET @TimerDT = SYSDATETIME(); 

 CREATE NONCLUSTERED INDEX [ixn_#SourceData] ON #SourceData([Name],[LOBCheckSum],[GMTDateTime]); 

 IF @LoggingLevel > 1
   SET @CreateIndex_ms = DATEDIFF(ms, @TimerDt, SYSDATETIME());

 IF @LoggingLevel > 1
   SET @TimerDT = SYSDATETIME(); 

 MERGE dbo.tParent AS p
 USING ( SELECT [Name]
              , [LOBCheckSum]
              , CAST([GMTDateTime] AS DATE) AS [GMTDate]
              , DATEPART(hour, [GMTDateTime]) AS [GMTHour]
              , MIN(LOB) AS [LOB] -- since LOBCheckSum determines equality no need to look in the LOB
              , COUNT(*) AS [Occurrences]  
         FROM #SourceData
         GROUP BY [NAME]
                , [LOBCheckSum]
                , CAST([GMTDateTime] AS DATE)
                , DATEPART(hour, [GMTDateTime])) AS t 
  ON p.[Name] = t.[Name]
  AND p.[LOBCheckSum] = t.[LOBCheckSum]
  AND p.[GMTDate] = t.[GMTDate]
  AND p.[GMTHour] = t.[GMTHour] 
  WHEN MATCHED THEN
      UPDATE SET [Occurrences] = p.[Occurrences] + 1
               , [LastUpdateDate] = CAST(SYSDATETIME() AS DATE)
               , [LastUpdateTime] = CAST(SYSDATETIME() AS TIME)     
  WHEN NOT MATCHED THEN
      INSERT ( [GMTDate] 
             , [GMTHour] 
             , [Name]
             , [LOBCheckSum]
             , [LOB] )
       VALUES
             ( t.[GMTDate]
             , t.[GMTHour]
             , t.[Name]
             , t.[LOBCheckSum]
             , t.[LOB] )
  OUTPUT Inserted.Id, Inserted.Name, Inserted.LOBCheckSum, Inserted.GMTDate, Inserted.GMTHour INTO @MergeMapper;

  SET @MergeParentRows = @@ROWCOUNT;

 IF @LoggingLevel > 1
   SET @MergeParent_ms = DATEDIFF(ms, @TimerDt, SYSDATETIME());

 IF @LoggingLevel > 1
   SET @TimerDT = SYSDATETIME(); 

  INSERT dbo.tChild 
   ( ParentId 
   , GMTDateTime
   , Value
   , Host
   , FloatingPoint
   , Boolean
   , Bytes )
  SELECT 
     p.Id
   , d.GMTDateTime
   , d.Value
   , d.Host
   , d.FloatingPoint
   , d.Boolean
   , d.Bytes
  FROM @MergeMapper p
  JOIN #SourceData d
  ON p.[Name] = d.[Name]
  AND p.[LOBCheckSum] = d.[LOBCheckSum]
  AND p.[GMTDate] = CAST(d.GMTDateTime AS DATE)
  AND p.[GMTHour] = DATEPART(hour, d.GMTDateTime);

  SET @InsertChildRows = @@ROWCOUNT;
  
 IF @LoggingLevel > 1
   SET @InsertChild_ms = DATEDIFF(ms, @TimerDt, SYSDATETIME());
    
END;


GO

/****** Object:  StoredProcedure [dbo].[TVPUpsert]    Script Date: 11/12/2010 05:57:51 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

---------------------------------------------------------
-- add the rows in provided table valued parameter
---------------------------------------------------------
CREATE PROCEDURE [dbo].[TVPUpsert]
    ( @TVP TVPTableType READONLY 
    , @LoggingLevel TINYINT
    , @LoadMethod NVARCHAR(128)
    , @TVPToTableVariableRows BIGINT OUTPUT
    , @TVPToTableVariable_ms BIGINT OUTPUT
    , @MergeParentRows BIGINT OUTPUT
    , @MergeParent_ms BIGINT OUTPUT
    , @InsertChildRows BIGINT OUTPUT
    , @InsertChild_ms BIGINT OUTPUT)
AS
BEGIN
 DECLARE @MergeMapper TABLE 
     ( Id BIGINT
     , Name VARCHAR(50)
     , LOBCheckSum INT
     , GMTDate DATE
     , GMTHour INT);

 DECLARE @TableVariable TVPTableType
       , @TimerDT DATETIME2;

 IF @LoggingLevel > 1
   SET @TimerDT = SYSDATETIME();
      
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
 FROM @TVP;
 SET @TVPToTableVariableRows = @@ROWCOUNT;

 IF @LoggingLevel > 1
  SET @TVPToTableVariable_ms = DATEDIFF(ms, @TimerDt, SYSDATETIME());
 
 IF @LoggingLevel > 1
   SET @TimerDT = SYSDATETIME();

 MERGE dbo.tParent AS p
 USING (SELECT aggon.RowNumber as RowNumber
             , tv.[Name]
             , CAST(tv.[GMTDateTime] AS DATE) AS [GMTDate]
             , DATEPART(hour, tv.[GMTDateTime]) AS [GMTHour]
             , tv.[LOB]
             , tv.[LOBCheckSum]
             , aggon.[Occurrences]
        FROM @TableVariable tv
        JOIN (SELECT RowNumber
                   , Occurrences
              FROM (SELECT [Name]
                         , [LOBCheckSum]
                         , CAST([GMTDateTime] AS DATE) AS [GMTDate] 
                         , DATEPART(hour, [GMTDateTime]) AS [GMTHour]
                         , MIN(RowNumber) AS [RowNumber]
                         , COUNT(*) AS [Occurrences]
                    FROM @TableVariable
                    GROUP BY [Name]
                           , [LOBCheckSum]
                           , CAST([GMTDateTime] AS DATE)
                           , DATEPART(hour, [GMTDateTime])) AS [first]) AS [aggon]
        ON tv.[RowNumber] = aggon.[RowNumber]) AS t
 ON p.[Name] = t.[Name]
 AND p.[LOBCheckSum] = t.[LOBCheckSum]
 AND p.[GMTDate] = t.[GMTDate]
 AND p.[GMTHour] = t.[GMTHour]
 WHEN MATCHED THEN
     UPDATE SET [Occurrences] += t.[Occurrences]
              , [LastUpdateDate] = CAST(SYSDATETIME() AS DATE)
              , [LastUpdateTime] = CAST(SYSDATETIME() AS TIME)     
 WHEN NOT MATCHED THEN
     INSERT ( [GMTDate] 
            , [GMTHour] 
            , [Name]
            , [LOBCheckSum]
            , [LOB] )
      VALUES
            ( t.[GMTDate]
            , t.[GMTHour]
            , t.[Name]
            , t.[LOBCheckSum]
            , t.[LOB])
 OUTPUT Inserted.Id, Inserted.Name, Inserted.LOBCheckSum, Inserted.GMTDate, Inserted.GMTHour INTO @MergeMapper;

 SET @MergeParentRows = @@ROWCOUNT;

 IF @LoggingLevel > 1
  SET @MergeParent_ms = DATEDIFF(ms, @TimerDt, SYSDATETIME());

 IF @LoggingLevel > 1
   SET @TimerDT = SYSDATETIME();

 INSERT dbo.tChild 
  ( ParentId 
  , GMTDateTime
  , Value
  , Host
  , FloatingPoint
  , Boolean
  , Bytes )
 SELECT 
    p.Id
  , d.GMTDateTime
  , d.Value
  , d.Host
  , d.FloatingPoint
  , d.Boolean
  , d.Bytes
 FROM @MergeMapper p
 JOIN @TableVariable d
 ON p.[Name] = d.[Name]
 AND p.[LOBCheckSum] = d.[LOBCheckSum]
 AND p.[GMTDate] = CAST(d.GMTDateTime AS DATE)
 AND p.[GMTHour] = DATEPART(hour, d.GMTDateTime); 

 SET @InsertChildRows = @@ROWCOUNT;

 IF @LoggingLevel > 1
  SET @InsertChild_ms = DATEDIFF(ms, @TimerDt, SYSDATETIME());

END;

GO


