-- SQLCMD substitution variables - always wrap literal values in "double" quotes
-- data file paths required for sccripts 1a, 1b and 1c, will often be the same
:SETVAR DATAPATH "C:\Program Files\Microsoft SQL Server\MSSQL10_50.ATER2\MSSQL\DATA" --"G:\MSSQL10_50.R2\MSSQL\DATA\"    
:SETVAR LOGPATH "C:\Program Files\Microsoft SQL Server\MSSQL10_50.ATER2\MSSQL\DATA"--"G:\MSSQL10_50.R2\MSSQL\DATA\"     
:SETVAR TARGETDB "target"                                                                
:SETVAR INITIATORDB "initiator"                                                          
:SETVAR INITIATORSQL ""                   -- Blank ("") if initiatordb local to target db
-- Any tTransferParameters values can be set here and used at db create time if usable.
:SETVAR MESSAGEQUANTITY 1             --                                                 
:SETVAR RowsToPush 10             --                                                 
-- broker dialog controls
:SETVAR RECEIVEMSGWAITTIME "00:00:00" -- add a delay between dialogs at the target       
:SETVAR NumberInitiatorTransactions 1 -- 1 is the recommendation here                    
:SETVAR SendTxnWaitTime "00:00:00"    -- add a delay between txns at the initiator       
:SETVAR NumberDialogs 1               --                                                 
:SETVAR DialogRecycleMaxMessages -1   --                                                 
:SETVAR MaxQueueReaders 1             --                                                 
:SETVAR MaxMessagesPerReceive 1       --                                                 
-- 0=none, 1=log steps (generate, fetch, load), 2=log + dump substep details to screen 
:SETVAR LOGGINGLEVEL 1                --  0= no logging, 1= log, 2 = more     
-- db configuration
:SETVAR DATECLUSTERING "NONCLUSTERED" -- CLUSTERED or /___ both cannot be clustered!     
:SETVAR IDCLUSTERING "CLUSTERED"      -- NONCLUSTERED \                                  
:SETVAR DATACOMPRESSION "ROW"         -- NONE, ROW or PAGE                                        
:SETVAR ROWVERSIONING "OFF"           -- "ON , "OFF"  causes fragmentation when on for updates    
:SETVAR FILLFACTOR 80                 -- dates are inserted out of order - even in nonclustered   
-- service broker configuration
:SETVAR DOMAINNAME ""            -- leave blank if no domain                                      
:SETVAR ENCRYPTION "DISABLED"    -- "ENABLED" "DISABLED" default is enabled!                      
:SETVAR INITIATORHOST "BILL_VU"  -- machine name of initiator db host only, NO SQL instance name  
:SETVAR INITIATORPORT 4022       -- 4022 is the one SQL Server uses if port not specified         
:SETVAR TARGETHOST "BILL_VU"     -- machine name of target db host only, NO SQL instance name     
:SETVAR TARGETPORT 4022          -- 4022 is the one SQL Server uses if port not specified         

SET NOCOUNT ON;
GO

USE master;
GO

IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '$(TARGETDB)')
  BEGIN
    CREATE DATABASE $(TARGETDB)
    ON ( NAME = $(TARGETDB),
         FILENAME = '$(DATAPATH)\$(TARGETDB).mdf',
         SIZE = 5000MB,
         MAXSIZE = 5000MB,
         FILEGROWTH = 50MB )
    LOG ON ( NAME = $(TARGETDB)_log,
             FILENAME = '$(LOGPATH)\$(TARGETDB)_log.ldf',
             SIZE = 500MB,
             MAXSIZE = 1000MB,
             FILEGROWTH = 50MB);

    ALTER DATABASE $(TARGETDB)
    SET RECOVERY SIMPLE;

    ALTER DATABASE $(TARGETDB)
    SET ENABLE_BROKER;

    ALTER DATABASE $(TARGETDB)
    SET READ_COMMITTED_SNAPSHOT $(ROWVERSIONING);

  END;
  
GO

USE $(TARGETDB);

GO

IF EXISTS(SELECT * FROM sys.databases
          WHERE database_id = DB_ID()
          AND name = '$(TARGETDB)'
          AND owner_sid <> 0x01)
  EXEC sp_changedbowner 'sa'

GO

IF OBJECT_ID('dbo.tTransferParameters','U') IS NULL
  CREATE TABLE dbo.tTransferParameters 
   ( MessageQuantity BIGINT NOT NULL
   , RowsToPush INT NOT NULL
   , ReceiveMsgWaitTime CHAR(8) NOT NULL       --q
   , NumberInitiatorTransactions INT NOT NULL  --q
   , SendTxnWaitTime CHAR(8) NOT NULL          --q
   , NumberDialogs INT NOT NULL                --q 
   , DialogRecycleMaxMessages BIGINT NOT NULL  --q
   , MaxQueueReaders INT NOT NULL              --q 
   , MaxMessagesPerReceive BIGINT NOT NULL     --q
   , LoggingLevel TINYINT NOT NULL
     CONSTRAINT ck__tTransferParameters__logging_level
     CHECK (LoggingLevel IN(0,1,2)));
GO

-- Insert default parameter values.
TRUNCATE TABLE dbo.tTransferParameters;
INSERT INTO dbo.tTransferParameters
  ( MessageQuantity
  , RowsToPush
  , ReceiveMsgWaitTime
  , NumberInitiatorTransactions
  , SendTxnWaitTime
  , NumberDialogs
  , DialogRecycleMaxMessages
  , MaxQueueReaders
  , MaxMessagesPerReceive 
  , LoggingLevel )
VALUES
  ( $(MESSAGEQUANTITY) 
  , $(RowsToPush)
  , '00:00:00'
  , 1
  , '00:00:00'
  , 1
  , 1
  , 1
  , -1 
  , $(LOGGINGLEVEL) );

GO

-- Check parameters.
DECLARE @MessageQuantity BIGINT;
DECLARE @NumberInitiatorTransactions INT;
DECLARE @NumberDialogs INT;
DECLARE @i BIGINT;
DECLARE @string VARCHAR(50);
SET @MessageQuantity = (SELECT MessageQuantity FROM dbo.tTransferParameters);
SET @NumberInitiatorTransactions = (SELECT NumberInitiatorTransactions FROM dbo.tTransferParameters);
SET @NumberDialogs = (SELECT NumberDialogs FROM dbo.tTransferParameters);
SET @i = @MessageQuantity / (@NumberDialogs * @NumberInitiatorTransactions);
SET @i = @i * @NumberDialogs * @NumberInitiatorTransactions;
IF @MessageQuantity > @i
BEGIN
     SET @i = @MessageQuantity - @i;
     SET @string = (SELECT CAST( @i AS VARCHAR(50)));
     PRINT 'Warning: @MessageQuantity is not evenly divisible by @NumberDialogs * @NumberInitiatorTransactions';
     PRINT @string + ' messages will not be sent to the target';
END;

GO

  IF OBJECT_ID('dbo.pGetIndexDetails','P') IS NOT NULL
    DROP PROCEDURE dbo.pGetIndexDetails;

GO
 CREATE PROCEDURE dbo.pGetIndexDetails
  ( @mode NVARCHAR(20) = 'SAMPLED' )
 AS
 BEGIN
 
   SELECT OBJECT_NAME(ps.[object_id],db_id('target')) AS [Table]
        , i.name AS [Index Name] 
        , ps.index_id
        , ps.index_type_desc
        , ps.alloc_unit_type_desc 
        , ps.record_count
        , ps.page_count
        , ps.avg_record_size_in_bytes
        , ps.avg_fragmentation_in_percent
        , ps.avg_page_space_used_in_percent
        , ps.compressed_page_count  
        , os.row_lock_wait_count + os.page_lock_wait_count + page_latch_wait_count + page_io_latch_wait_count AS [wait count]
   FROM sys.dm_db_index_physical_stats(db_id('$(TARGETDB)'),object_id('$(TARGETDB).dbo.tParent'),null,null,@Mode) ps
   JOIN sys.dm_db_index_operational_stats(db_id('$(TARGETDB)'),object_id('$(TARGETDB).dbo.tParent'),null,null) os
   ON ps.object_id = os.Object_id
   AND ps.index_id = os.index_id 
   JOIN target.sys.indexes i 
   ON ps.object_id = i.Object_id
   AND ps.index_id = i.index_id
   UNION ALL
   SELECT OBJECT_NAME(ps.[object_id],db_id('$(TARGETDB)'))
        , i.name  
        , ps.index_id
        , ps.index_type_desc
        , ps.alloc_unit_type_desc 
        , ps.record_count
        , ps.page_count
        , ps.avg_record_size_in_bytes
        , ps.avg_fragmentation_in_percent
        , ps.avg_page_space_used_in_percent
        , ps.compressed_page_count 
        , os.row_lock_wait_count + os.page_lock_wait_count + page_latch_wait_count + page_io_latch_wait_count AS [wait count]
   FROM sys.dm_db_index_physical_stats(db_id('$(TARGETDB)'),object_id('$(TARGETDB).dbo.tChild'),null,null,@Mode) ps
   JOIN sys.dm_db_index_operational_stats(db_id('$(TARGETDB)'),object_id('$(TARGETDB).dbo.tChild'),null,null) os
   ON ps.object_id = os.Object_id
   AND ps.index_id = os.index_id 
   JOIN target.sys.indexes i 
   ON ps.object_id = i.Object_id
   AND ps.index_id = i.index_id
   ORDER BY [Table], [index_id] ;
  
  END;

GO

IF OBJECT_ID('dbo.tActivityHistory', 'U') IS NULL 
    CREATE TABLE dbo.tActivityHistory 
     ( Id INT IDENTITY(1,1) NOT NULL
     , LoadMethod NVARCHAR(128)	
     , StartDT DATETIME2
     , Load_ms BIGINT
     , LoadedRows BIGINT
     , Elapsed_ms BIGINT
     , MessageQuantity BIGINT
     , RowsToPush INT
     , ReceiveMsgWaitTime CHAR(8)
     , NumberInitiatorTransactions INT
     , SendTxnWaitTime CHAR(8)
     , NumberDialogs INT
     , DialogRecycleMaxMessages INT
     , MaxQueueReaders TINYINT
     , MaxMessagesPerReceive INT
     , LoggingLevel TINYINT
     , RecCreateDT DATETIME2
       CONSTRAINT dft_ActivityHistory__RecCreateDT
       DEFAULT (SYSDATETIME())
     , RecCreateUser SYSNAME
       CONSTRAINT dft_ActivityHistory_RecCreateUser
       DEFAULT (ORIGINAL_LOGIN()) 
     , CONSTRAINT pkc_ActivityHistory
       PRIMARY KEY (Id) )
    WITH (DATA_COMPRESSION = $(DATACOMPRESSION));
GO

IF OBJECT_ID('ck_LoadedRows') IS NULL 
  ALTER TABLE dbo.tActivityHistory 
  Add CONSTRAINT ck_LoadedRows 
  CHECK (LoadedRows IN (1,5,10,50,100,500,1000,5000,10000,50000,100000)); 

GO

IF OBJECT_ID('dbo.tReceiveHistory', 'U') IS NULL 
    CREATE TABLE dbo.tReceiveHistory 
     ( Id INT IDENTITY(1,1) NOT NULL
     , ActivityHistoryId INT
     , LoadMethod NVARCHAR(128)	
     , Receive_ms BIGINT
     , MessageToTablevariableRows BIGINT
     , MessageToTablevariable_ms BIGINT 
     , TVPToTableVariableRows BIGINT
     , TVPToTableVariable_ms BIGINT
     , MergeParentRows BIGINT
     , MergeParent_ms BIGINT
     , InsertChildRows BIGINT
     , InsertChild_ms BIGINT 
     , RecCreateDt DATETIME2
       CONSTRAINT dft_ReceiveHistory__RecCreateDt
       DEFAULT (SYSDATETIME())
     , RecCreateUser NVARCHAR(128)
       CONSTRAINT dft_ReCeiveHistory_RecCreateUser
       DEFAULT (ORIGINAL_LOGIN())

     , CONSTRAINT pkc_ReceiveHistory
       PRIMARY KEY (Id) )
  
       WITH (DATA_COMPRESSION = $(DATACOMPRESSION));
GO

-- Table to count processed messages.
IF EXISTS (SELECT name FROM sys.tables WHERE name = 'tTargetMessageCounter')
	DROP TABLE tTargetMessageCounter;
GO

CREATE TABLE tTargetMessageCounter (counter BIGINT NOT NULL);
GO

INSERT INTO tTargetMessageCounter VALUES (0);
GO

-- Table to store processing errors.
IF EXISTS (SELECT name FROM sys.tables WHERE name = 'tTargetProcessingErrors')
	DROP TABLE tTargetProcessingErrors;
GO

CREATE TABLE dbo.tTargetProcessingErrors 
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
  , [CreateDT] DATETIME2 
    CONSTRAINT dft_tTargetProcessingErrors__CreateDT
    DEFAULT (SYSDATETIME())
  , CONSTRAINT pk_tTargetProcessingErrors 
    PRIMARY KEY (Id))
WITH (DATA_COMPRESSION = $(DATACOMPRESSION));
GO

IF OBJECT_ID('dbo.pGetQueueSizes','P') IS NOT NULL
    DROP PROCEDURE dbo.pGetQueueSizes

GO

---------------------------------------------------------------------
-- Get size of a message queue.
-- Method used is faster than SQL count operator.
---------------------------------------------------------------------
CREATE PROCEDURE dbo.pGetQueueSizes 
AS
BEGIN
    SELECT [initiator_queue] AS [Initiator Queue Msg Count]
         , [sysxmitqueue] AS [Transmission Queue Msg Count]
         , [target_queue] AS [Target Queue Msg Count]
    FROM ( SELECT q.name AS name, p.rows
           FROM $(INITIATORDB).sys.objects AS o
           JOIN $(INITIATORDB).sys.partitions AS p 
           ON p.object_id = o.object_id
           JOIN $(INITIATORDB).sys.objects AS q 
           ON o.parent_object_id = q.object_id
           WHERE q.name = 'initiator_queue'
           AND p.index_id = 1
             UNION ALL 
           SELECT o.name AS name, p.rows
           FROM sys.objects AS o
           JOIN sys.partitions AS p 
           ON p.object_id = o.object_id
           WHERE o.name = 'sysxmitqueue'
             UNION ALL
           SELECT q.name AS name, p.rows
           FROM sys.objects AS o
           JOIN sys.partitions AS p 
           ON p.object_id = o.object_id
           JOIN sys.objects AS q 
           ON o.parent_object_id = q.object_id
           WHERE q.name = 'target_queue'
           AND p.index_id = 1 ) AS SourceData
    PIVOT (SUM(rows) FOR name IN ([initiator_queue], [sysxmitqueue], [target_queue])) AS PivotTable     
END;
GO

IF OBJECT_ID('dbo.pGetBrokerStatus','P') IS NOT NULL
    DROP PROCEDURE dbo.pGetBrokerStatus

GO

CREATE PROCEDURE dbo.pGetBrokerStatus 
AS
--NOTE: leave the square brackets on the activation procrdure literals!
IF EXISTS (SELECT *
           FROM target.sys.service_queues 
           WHERE [name] = 'initiator_queue'
           AND ([activation_procedure] <> '[dbo].[pInitiatorActivationProcedure]'
                OR [is_activation_enabled] = 0
                OR [is_receive_enabled] = 0
                OR [is_enqueue_enabled]  = 0))
  BEGIN
    IF EXISTS (SELECT *
               FROM target.sys.service_queues 
               WHERE [name] = 'initiator_queue'
               AND [activation_procedure] <> '[dbo].[pInitiatorActivationProcedure]')
      RAISERROR ('Unexpected Activation Procedure for initiator_queue', 16, 1);          
    ELSE IF EXISTS (SELECT *
                    FROM target.sys.service_queues 
                    WHERE [name] = 'initiator_queue'
                    AND [is_activation_enabled] = 0)
      RAISERROR ('Service Broker "initiator queue" needs attention before messages can be received!', 16, 1);          
    ELSE
      RAISERROR ('Service Broker "initiator queue" needs attention before messages can be sent!', 16, 1);          
  END;

IF EXISTS (SELECT *
           FROM target.sys.service_queues 
           WHERE [name] = 'target_queue'
           AND ([activation_procedure] <> '[dbo].[pTargetActivationProcedure]'
                OR [is_activation_enabled] = 0
                OR [is_receive_enabled] = 0
                OR [is_enqueue_enabled]  = 0))
  BEGIN 
    IF EXISTS (SELECT *
               FROM target.sys.service_queues 
               WHERE [name] = 'target_queue'
               AND [activation_procedure] <> '[dbo].[pTargetActivationProcedure]')
      RAISERROR ('Unexpected Activation Procedure for target_queue', 16, 1);          
    ELSE IF EXISTS (SELECT *
                    FROM target.sys.service_queues 
                    WHERE [name]  = 'target_queue'
                    AND [is_activation_enabled] = 0)
      RAISERROR ('Service Broker "target_queue" needs attention before messages can be received!', 16, 1);          
    ELSE
      RAISERROR ('Service Broker "target_queue" needs attention before messages can be sent!', 16, 1);          
  END;

GO

IF OBJECT_ID('dbo.pGetActivityForLoadMethod','P') IS NOT NULL
    DROP PROCEDURE dbo.pGetActivityForLoadMethod

GO

CREATE PROCEDURE dbo.pGetActivityForLoadMethod
 ( @LoadMethod NVARCHAR(128) ) 
AS
BEGIN
    SELECT a.LoadMethod
      , a.LoadedRows
      , a.Load_ms
      , r.Receive_ms
      , a.Load_ms/COALESCE(  NULLIF(a.LoadedRows,0)
                           , NULLIF(DATEDIFF(ms, a.StartDT, ISNULL(r.RecCreateDT, a.RecCreateDT)),0)
                           , 1.0 ) AS [Load_ms/row]
      , r.Receive_ms/COALESCE(NULLIF(r.InsertChildRows, 0), NULLIF(a.LoadedRows, 0), 1.0) AS [Receive_ms/row]
      , a.Elapsed_ms - a.Load_ms AS [Other_ms]
      , DATEDIFF(ms, a.StartDT, ISNULL(r.RecCreateDT, a.RecCreateDT)) AS [Elapsed_ms]
      , a.StartDT
    FROM dbo.tActivityHistory a
    LEFT JOIN dbo.tReceiveHistory r
    on a.Id = r.ActivityHistoryId
    WHERE a.LoadMethod = @LoadMethod
    ORDER BY a.RecCreateDt DESC;
END;
GO

------------ Type that defines the source data --------------
-- cannot drop this w/out dropping all procedures that use it

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

------------------------- target tables -----------------------------

-- tables to receive tParent/tChild data
IF OBJECT_ID('dbo.tChild', 'U') IS NOT NULL
    DROP TABLE dbo.tChild;
GO
IF OBJECT_ID('dbo.tParent', 'U') IS NOT NULL
    DROP TABLE dbo.tParent;
GO
CREATE TABLE dbo.tParent
 ( Id BIGINT IDENTITY(1,1)
 , GMTDate DATE
 , GMTHour INT
 , Name VARCHAR(50)
 , LOBCheckSum INT 
 , LOB NVARCHAR(MAX)
 , Occurrences BIGINT
   CONSTRAINT dft_tParent__Occurrences
   DEFAULT (1)
 , LastUpdateDate DATE 
   CONSTRAINT dft_tParent__UpdateDate
   DEFAULT (CAST(SYSDATETIME() AS DATE))
 , LastUpdateTime TIME
   CONSTRAINT dft_tParent__UpdateTime
   DEFAULT (CAST(SYSDATETIME() AS TIME))
 , CreateDate DATE 
   CONSTRAINT dft_tParent__CreateDate
   DEFAULT (CAST(SYSDATETIME() AS DATE))
 , CreateTime TIME
   CONSTRAINT dft_tParent__CreateTime
   DEFAULT (CAST(SYSDATETIME() AS TIME))
 , CONSTRAINT pk_tParent__Id
   PRIMARY KEY $(IDCLUSTERING) (Id)
 , CONSTRAINT uk_tParent__Name__LOBSize__GMTDate__GMTHour
   UNIQUE (Name, LOBCheckSum, GMTDate, GMTHour))
WITH (DATA_COMPRESSION = $(DATACOMPRESSION));

GO

CREATE $(DATECLUSTERING) INDEX ix_tParent__GMTDate__GMTHour
ON dbo.tParent(GMTDate, GMTHour)
WITH  (DATA_COMPRESSION = $(DATACOMPRESSION));

GO

CREATE TABLE dbo.tChild
 ( Id BIGINT IDENTITY(1,1)
 , ParentId BIGINT
 , GMTDateTime DATETIME2
 , Value INT
 , Host NVARCHAR(128)
 , FloatingPoint REAL
 , Boolean BIT
 , Bytes VARBINARY(60)
 , CreateDate DATE 
   CONSTRAINT dft_tChild__CreateDate
   DEFAULT (CAST(SYSDATETIME() AS DATE))
 , CreateTime TIME
   CONSTRAINT dft_tChild__CreateTime
   DEFAULT (CAST(SYSDATETIME() AS TIME))
 , CONSTRAINT pk_tChild__Id
   PRIMARY KEY $(IDCLUSTERING) (Id)
 , CONSTRAINT fk__tChild__TO__Parent__Id
   FOREIGN KEY (ParentId) REFERENCES dbo.tParent (Id))
WITH (DATA_COMPRESSION = $(DATACOMPRESSION));

GO

 CREATE $(DATECLUSTERING) INDEX ix_tChild__GMTDateTime__ParentId 
 ON dbo.tChild(GMTDateTime, ParentId)
 WITH (DATA_COMPRESSION = $(DATACOMPRESSION));

GO

----------------------------- END target tables --------------------------------

-- the function could be moved to any db on any 2005/2008 and still produce rows
IF OBJECT_ID (N'dbo.fGetTableVariable') IS NOT NULL
   DROP FUNCTION dbo.fGetTableVariable
GO
/*
select * from target.dbo.fGetTableVariable(1,1)     --gets row 1
select * from target.dbo.fGetTableVariable(5,3)     -- gets rows 5,6,7
select * from target.dbo.fGetTableVariable(100,100) -- gets rows 100 through 199
*/
CREATE FUNCTION dbo.fGetTableVariable(@FirstRow int, @RowCount int)
RETURNS @TableVariable TABLE 
(
    -- columns returned by the function
      RowNumber INT
    , Host NVARCHAR(128)
    , GMTDateTime DATETIME
    , Name VARCHAR(50)
    , Value INT 
    , FloatingPoint REAL
    , Boolean BIT
    , Bytes VARBINARY(60)
    , LOB NVARCHAR(MAX)
    , LOBCheckSum INT)
AS
-- body of the function
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
        , LOBCheckSum )
    SELECT [RowNumber]
         , [Host]
         , [GMTDateTime]
         , [Name]
         , [Value]
         , [FloatingPoint]
         , [Boolean]
         , [Bytes] 
         , [LOB]
         , [LOBCheckSum]
    FROM $(INITIATORDB).dbo.SourceData  
    WHERE RowNumber BETWEEN @FirstRow AND (@FirstRow + @RowCount - 1)
    ORDER BY RowNumber;
  
  RETURN; 

END;

GO

IF OBJECT_ID('dbo.fBinaryMarshalRow', 'fn') IS NOT NULL
    DROP FUNCTION dbo.fBinaryMarshalRow;
GO
CREATE FUNCTION dbo.fBinaryMarshalRow
  ( @RowNumber INT
  , @Host NVARCHAR(128)
  , @GMTDateTime DATETIME
  , @Name VARCHAR(50)
  , @Value INT
  , @FloatingPoint REAL
  , @Boolean BIT
  , @Bytes VARBINARY(60)
  , @LOB NVARCHAR(MAX)
  , @LOBCheckSum INT) 
RETURNS VARBINARY(MAX)
AS
BEGIN
  DECLARE @BINARY_ARRAY VARBINARY(MAX);

  SET @BINARY_ARRAY = CAST(CAST('row' AS CHAR(3)) AS VARBINARY(MAX)) 
       + CAST(@RowNumber AS VARBINARY(MAX))                                                             
       + CAST(CAST(DATALENGTH(CAST(@Host AS VARBINARY(MAX))) AS TINYINT) AS VARBINARY(MAX))             
       + CAST(@Host AS VARBINARY(MAX))                                                                  
       + CAST(@GMTDateTime AS VARBINARY(MAX))                                                        
       + CAST(CAST(DATALENGTH(CAST(@Name AS VARBINARY(MAX))) AS TINYINT) AS VARBINARY(MAX))             
       + CAST(@Name AS VARBINARY(MAX))                                                                  
       + CAST(@Value AS VARBINARY(MAX))                                                                 
       + CAST(STR(@FloatingPoint, 20, 16) AS VARBINARY(MAX))  
       + CAST(@Boolean AS VARBINARY(MAX))
       + CAST(CAST(DATALENGTH(CAST(@Bytes AS VARBINARY(MAX))) AS TINYINT) AS VARBINARY(MAX))            
       + CAST(@Bytes AS VARBINARY(MAX))                                                                 
       + CAST(CAST(DATALENGTH(CAST(@LOB AS VARBINARY(MAX))) AS INT) AS VARBINARY(MAX))                  
       + CAST(@LOB AS VARBINARY(MAX))                                                                   
       + CAST(DATALENGTH(@LOB) AS VARBINARY(MAX));  

  RETURN @BINARY_ARRAY;
  
END;

GO

IF OBJECT_ID('dbo.fBinaryUnmarshalRow', 'tf') IS NOT NULL
    DROP Function dbo.fBinaryUnmarshalRow;
GO
-- yet another place where the SourceData type must be maintained
CREATE FUNCTION dbo.fBinaryUnmarshalRow
  ( @Datagram VARBINARY(MAX) ) 
RETURNS @TableVariable TABLE
  ( RowNumber INT
  , Host NVARCHAR(128)
  , GMTDateTime DATETIME
  , Name VARCHAR(50)
  , Value INT
  , FloatingPoint REAL
  , Boolean BIT
  , Bytes VARBINARY(60)
  , LOB NVARCHAR(MAX)
  , LOBCheckSum INT) 
AS
BEGIN

  DECLARE @HostLength TINYINT
        , @NameLength TINYINT
        , @BytesLength TINYINT 
        , @LOBLength INT;

  
  SET @HostLength = CAST(SUBSTRING( @Datagram
                                  , 5
                                  , 1) AS TINYINT);
  SET @NameLength = CAST(SUBSTRING( @Datagram
                                  , 5 + 1 + @HostLength + 8  
                                  , 1) AS TINYINT) 
  SET @BytesLength = CAST(SUBSTRING( @Datagram 
                                    , 5 + 1 + @HostLength + 8 + 1 + @NameLength + 4 + 20
                                    , 1) AS TINYINT)
  SET @LOBLength = CAST(SUBSTRING( @Datagram
                                  , 5 + 1 + @HostLength + 8 + 1 + @NameLength + 4 + 20 + 1 + @BytesLength
                                  , 4) AS INT);
  INSERT INTO @TableVariable      -- bytes
     ( RowNumber                    -- 4
     , Host                         -- @HostLength(1) 
     , GMTDateTime               -- 8
     , Name                         -- @Namelength (1)  
     , Value                        -- 4 
     , FloatingPoint                -- 20
     , Boolean                      -- 1
     , Bytes                        -- @BytesLength(1)
     , LOB                          -- @LOBLength(4)
     , LOBCheckSum )                    -- 4 
  SELECT  CAST(SUBSTRING( b.binrow
                        , 1
                        , 4) AS INT) AS [RowNumber] 
        , CAST(SUBSTRING( b.binrow
                        , 5 + 1
                        , @HostLength) AS NVARCHAR(128)) AS [Host]
        , CAST(SUBSTRING( b.binrow
                         , 5 + 1 + @HostLength 
                         , 8) AS DATETIME) AS [GMTDateTime]
        , CAST(SUBSTRING( b.binrow
                        , 5 + 1 + @HostLength + 8 + 1
                        , @NameLength) AS VARCHAR(50)) AS [Name]
        , CAST(SUBSTRING( b.binrow
                        , 5 + 1 + @HostLength + 8 + 1 + @NameLength 
                        , 4) AS INT) AS [Value]
        , CAST(CAST(SUBSTRING( b.binrow
                             , 5 + 1 + @HostLength + 8 + 1 + @NameLength + 4
                             , 20) AS CHAR(20)) AS REAL) AS [FloatingPoint]
        , CAST(SUBSTRING( b.binrow
                        , 5 + 1 + @HostLength + 8 + 1 + @NameLength + 4 + 20
                        , 1) AS BIT) AS [Boolean]
        , CAST(SUBSTRING( b.binrow
                      , 5 + 1 + @HostLength + 8 + 1 + @NameLength + 4 + 20 + 1
                      , @BytesLength) AS VARBINARY(60)) AS [Bytes]
        , CAST(SUBSTRING( b.binrow
                      , 5 + 1 + @HostLength + 8 + 1 + @NameLength + 4 + 20 + 1 + @BytesLength + 4 
                      , @LOBLength) AS NVARCHAR(MAX)) AS [LOB]
        , CAST(SUBSTRING( b.binrow
                      , 5 + 1 + @HostLength + 8 + 1 + @NameLength + 4 + 20 + 1 + @BytesLength + 4 + @LOBLength
                      , 4) AS INT) AS [LOBCheckSum]
  FROM (SELECT @Datagram as binrow) b;

RETURN;

END;
                            
GO

IF OBJECT_ID('dbo.fBinaryMarshalTVP', 'fn') IS NOT NULL
    DROP FUNCTION dbo.fBinaryMarshalTVP;
GO
CREATE FUNCTION dbo.fBinaryMarshalTVP
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

IF OBJECT_ID('dbo.fBinaryUnmarshalTVP', 'tf') IS NOT NULL
    DROP Function dbo.fBinaryUnmarshalTVP;
GO
-- yet another place where the SourceData type must be maintained
CREATE FUNCTION dbo.fBinaryUnmarshalTVP
  ( @bigbin VARBINARY(MAX) ) 
RETURNS @TableVariable TABLE
  ( RowNumber INT
  , Host NVARCHAR(128)
  , GMTDateTime DATETIME
  , Name VARCHAR(50)
  , Value INT
  , FloatingPoint REAL
  , Boolean BIT
  , Bytes VARBINARY(60)
  , LOB NVARCHAR(MAX)
  , LOBCheckSum INT) 
AS
BEGIN

DECLARE @RowsInBlob BIGINT
      , @binrow VARBINARY(MAX)
      , @binrowlength INT
      , @blob VARBINARY(MAX)
      , @HostLength TINYINT
      , @NameLength TINYINT
      , @BytesLength TINYINT 
      , @LOBLength INT
      , @RowCount BIGINT;

-- strip the header
SELECT @RowsInBlob = CAST( SUBSTRING( b.blob, 1, 8) AS BIGINT)
     , @blob = SUBSTRING( b.blob, 9 , DATALENGTH(b.blob))
FROM (SELECT @bigbin AS blob) b;    

--SELECT @RowsInBlob as [@RowsInBlob], @blob;

SET @RowCount = 0 
WHILE @RowCount < @RowsInBlob
 BEGIN
  SELECT @binrow = CAST(SUBSTRING( b.blob
                                 , 9
                                 , CAST(SUBSTRING( b.blob
                                                 , 1
                                                 , 8) AS BIGINT)) AS VARBINARY(MAX))
       , @blob = CAST( SUBSTRING( b.blob
                                , 9 + CAST(SUBSTRING( b.blob
                                                    , 1
                                                    , 8) AS BIGINT)
                                , DATALENGTH(b.blob)) AS VARBINARY(MAX))
   FROM (SELECT @blob AS blob) b;    
  
   SET @HostLength = CAST(SUBSTRING( @binrow
                                   , 5
                                   , 1) AS TINYINT);
   SET @NameLength = CAST(SUBSTRING( @binrow
                                   , 5 + 1 + @HostLength + 8  
                                   , 1) AS TINYINT) 
   SET @BytesLength = CAST(SUBSTRING( @binrow
                                     , 5 + 1 + @HostLength + 8 + 1 + @NameLength + 4 + 20
                                     , 1) AS TINYINT)
   SET @LOBLength = CAST(SUBSTRING( @binrow
                                   , 5 + 1 + @HostLength + 8 + 1 + @NameLength + 4 + 20 + 1 + @BytesLength
                                   , 4) AS INT);

    INSERT INTO @TableVariable      -- bytes
       ( RowNumber                    -- 4
       , Host                         -- @HostLength(1) 
       , GMTDateTime               -- 8
       , Name                         -- @Namelength (1)  
       , Value                        -- 4 
       , FloatingPoint                -- 20
       , Boolean                      -- 1
       , Bytes                        -- @BytesLength(1)
       , LOB                          -- @LOBLength(4)
       , LOBCheckSum )                    -- 4 

   SELECT  CAST(SUBSTRING( b.binrow
                         , 1
                         , 4) AS INT) AS [RowNumber] 
         , CAST(SUBSTRING( b.binrow
                         , 5 + 1
                         , @HostLength) AS NVARCHAR(128)) AS [Host]
         , CAST(SUBSTRING( b.binrow
                          , 5 + 1 + @HostLength 
                          , 8) AS DATETIME) AS [GMTDateTime]
         , CAST(SUBSTRING( b.binrow
                         , 5 + 1 + @HostLength + 8 + 1
                         , @NameLength) AS VARCHAR(50)) AS [Name]
         , CAST(SUBSTRING( b.binrow
                         , 5 + 1 + @HostLength + 8 + 1 + @NameLength 
                         , 4) AS INT) AS [Value]
         , CAST(CAST(SUBSTRING( b.binrow
                              , 5 + 1 + @HostLength + 8 + 1 + @NameLength + 4
                              , 20) AS CHAR(20)) AS REAL) AS [FloatingPoint]
         , CAST(SUBSTRING( b.binrow
                         , 5 + 1 + @HostLength + 8 + 1 + @NameLength + 4 + 20
                         , 1) AS BIT) AS [Boolean]
         , CAST(SUBSTRING( b.binrow
                       , 5 + 1 + @HostLength + 8 + 1 + @NameLength + 4 + 20 + 1
                       , @BytesLength) AS VARBINARY(60)) AS [Bytes]
         , CAST(SUBSTRING( b.binrow
                       , 5 + 1 + @HostLength + 8 + 1 + @NameLength + 4 + 20 + 1 + @BytesLength + 4 
                       , @LOBLength) AS NVARCHAR(MAX)) AS [LOB]
         , CAST(SUBSTRING( b.binrow
                       , 5 + 1 + @HostLength + 8 + 1 + @NameLength + 4 + 20 + 1 + @BytesLength + 4 + @LOBLength
                       , 4) AS INT) AS [LOBCheckSum]
   FROM (SELECT @binrow as binrow) b;

   SET @RowCount += 1;

  END;

RETURN; 

END;

GO

IF OBJECT_ID (N'dbo.pRowMerge', N'P') IS NOT NULL
    DROP PROCEDURE dbo.pRowMerge;
GO
-----------------------------------------
-- add the columnar values provided 
-----------------------------------------
CREATE PROCEDURE dbo.pRowMerge
    ( @RowNumber INT                                                                                                                                                                    
    , @Host NVARCHAR(128)
    , @GMTDateTime DATETIME
    , @Name VARCHAR(50)
    , @Value INT 
    , @FloatingPoint REAL
    , @Boolean BIT
    , @Bytes VARBINARY(60)
    , @LOB NVARCHAR(MAX)
    , @LOBCheckSum INT 
    , @LoggingLevel TINYINT
    , @LoadMethod NVARCHAR(128) )
AS
BEGIN

  DECLARE @TimerDT DATETIME2;

  DECLARE @MergeParentRows INT
        , @MergeParent_ms INT    
        , @InsertChildRows INT
        , @InsertChild_ms INT;

  DECLARE @MergeMapper TABLE 
      ( Id BIGINT );

  BEGIN TRY

    BEGIN TRANSACTION;

      SET @MergeParentRows = 0;
      SET @MergeParent_ms = 0; 
      SET @InsertChildRows = 0;
      SET @InsertChild_ms = 0; 
      
      IF @LoggingLevel > 1
        SET @TimerDT = SYSDATETIME();

      MERGE dbo.tParent AS p 
      USING (SELECT @RowNumber as [RowNumber]
                  , @Name AS [Name]
                  , CAST(@GMTDateTime AS DATE) AS [GMTDate]
                  , DATEPART(hour, @GMTDateTime) AS [GMTHour]
                  , @LOB AS [LOB]
                  , @LOBCheckSum AS [LOBCheckSum]
                  , 1 AS [Occurrences]) AS t
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
      OUTPUT Inserted.Id INTO @MergeMapper;

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
      SELECT Id
          , @GMTDateTime
          , @Value
          , @Host
          , @FloatingPoint
          , @Boolean
          , @Bytes
      FROM @MergeMapper; 

      SET @InsertChildRows = @@ROWCOUNT;

      IF @LoggingLevel > 1
        SELECT @MergeParentRows AS [@MergeParentRows]
             , @MergeParent_ms AS [@MergeParent_ms]
             , @InsertChildRows AS [@InsertChildRows]
             , DATEDIFF(ms, @TimerDt, SYSDATETIME()) AS [@InsertChild_ms];
    
    COMMIT TRANSACTION;
    
    RETURN @InsertChildRows;

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

     -- don't mess with anyone else's xacts
     IF XACT_STATE() <> 0
       ROLLBACK TRANSACTION;

     RAISERROR ('dbo.pRowMerge Msg %d, Level %d, State %d, %s Line %d %s'
               ,16, 1, @Number, @Severity, @State, @Proc, @Line, @Message)

  END CATCH

END;

GO

IF OBJECT_ID (N'dbo.pRowUpsert', N'P') IS NOT NULL
    DROP PROCEDURE dbo.pRowUpsert;
GO
-----------------------------------------
-- add the columnar values provided 
-----------------------------------------
CREATE PROCEDURE dbo.pRowUpsert
    ( @RowNumber INT
    , @Host NVARCHAR(128)
    , @GMTDateTime DATETIME
    , @Name VARCHAR(50)
    , @Value INT 
    , @FloatingPoint REAL
    , @Boolean BIT
    , @Bytes VARBINARY(60)
    , @LOB NVARCHAR(MAX)
    , @LOBCheckSum INT 
    , @LoggingLevel TINYINT
    , @LoadMethod NVARCHAR(128) )
AS
BEGIN

  DECLARE @TimerDT DATETIME2;

  DECLARE @MergeParentRows INT
        , @MergeParent_ms INT
        , @InsertChildRows INT
        , @InsertChild_ms INT;

  DECLARE @UpsertMapper TABLE 
      ( Id BIGINT );

  BEGIN TRY
    
   SET @MergeParentRows = 0;
   SET @MergeParent_ms = 0; 
   SET @InsertChildRows = 0;
   SET @InsertChild_ms = 0; 
   
   IF @LoggingLevel > 1
     SET @TimerDT = SYSDATETIME();

    BEGIN TRANSACTION;

     UPDATE p
     SET [Occurrences] += 1
       , [LastUpdateDate] = CAST(SYSDATETIME() AS DATE)
       , [LastUpdateTime] = CAST(SYSDATETIME() AS TIME)     
     OUTPUT inserted.Id 
     INTO @UpsertMapper
     FROM dbo.tParent p
     WHERE p.[Name] = @Name
     AND p.[LOBCheckSum] = @LOBCheckSum
     AND p.[GMTDate] = CAST(@GMTDateTime AS DATE)
     AND p.[GMTHour] = DATEPART(hour, @GMTDateTime);

     SET @MergeParentRows = @@ROWCOUNT    

     IF @MergeParentRows = 0
     
       BEGIN
     
         INSERT dbo.tParent 
           ( [Name]
           , [GMTDate]
           , [GMTHour] 
           , [LOB]
           , [LOBCheckSum] )
         OUTPUT inserted.Id 
         INTO @UpsertMapper 
         VALUES 
           ( @Name
           , CAST(@GMTDateTime AS DATE)
           , DATEPART(hour, @GMTDateTime)
           , @LOB
           , @LOBCheckSum);
                   
       END
           
     SET @MergeParentRows = (SELECT COUNT(*) FROM @UpsertMapper);

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
     SELECT Id
         , @GMTDateTime
         , @Value
         , @Host
         , @FloatingPoint
         , @Boolean
         , @Bytes
     FROM @UpsertMapper; 

     SET @InsertChildRows = @@ROWCOUNT;

     IF @LoggingLevel > 1
       SELECT @MergeParentRows AS [@MergeParentRows]
            , @MergeParent_ms AS [@MergeParent_ms]
            , @InsertChildRows AS [@InsertChildRows]
            , DATEDIFF(ms, @TimerDt, SYSDATETIME()) AS [@InsertChild_ms];

     COMMIT TRANSACTION;
    
     RETURN @InsertChildRows;
     
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
          , @Proc = ISNULL('Procedure ' + SPACE(1) + ERROR_PROCEDURE() + SPACE(1), '')
          , @Line = ERROR_LINE()
          , @Message = N':' + CHAR(13) + CHAR(10) + ERROR_MESSAGE();

     IF XACT_STATE() <> 0
       ROLLBACK TRANSACTION;

     RAISERROR ('dbo.pRowUpsert: Msg %d, Level %d, State %d, %s Line %d %s'
               ,16 ,1 ,@Number, @Severity, @State, @Proc, @Line, @Message)

  END CATCH

END;

GO

IF OBJECT_ID (N'dbo.pTVPMerge', N'P') IS NOT NULL
    DROP PROCEDURE dbo.pTVPMerge;
GO
---------------------------------------------------------
-- add the rows in provided table valued parameter
---------------------------------------------------------
CREATE PROCEDURE dbo.pTVPMerge
    ( @TVP TVPTableType READONLY 
    , @LoggingLevel TINYINT
    , @LoadMethod NVARCHAR(128) )
AS

BEGIN

  DECLARE @TimerDT DATETIME2;

  DECLARE @MergeParentRows INT
        , @MergeParent_ms INT    
        , @InsertChildRows INT
        , @InsertChild_ms INT;

  CREATE TABLE #MergeMapper 
    ( Id BIGINT PRIMARY KEY
    , Name VARCHAR(50)
    , LOBCheckSum INT
    , GMTDate DATE
    , GMTHour INT
    , UNIQUE (Name, LOBCheckSum, GMTDate, GMTHour));

  CREATE TABLE #TempTable
   ( Name VARCHAR(50)
   , LOBCheckSum NVARCHAR(128)
   , GMTDate DATE
   , GMTHour INT 
   , Occurrences INT 
   , RowNumber INT
   , PRIMARY KEY ([Name],[LOBCheckSum],[GMTDate],[GMTHour])) 

  IF @LoggingLevel > 1
    SET @TimerDT = SYSDATETIME();
       
  INSERT #TempTable 
    ( Name
    , LOBCheckSum
    , GMTDate
    , GMTHour
    , Occurrences 
    , RowNumber)
  SELECT Name
       , LOBCheckSum
       , CAST(GMTDateTime AS DATE)
       , DATEPART(hour, GMTDateTime)
       , COUNT(*)
       , MIN(RowNumber) 
  FROM @TVP
  GROUP BY Name
       , LOBCheckSum
       , CAST(GMTDateTime AS DATE)
       , DATEPART(hour, GMTDateTime);

  BEGIN TRY
 
    BEGIN TRANSACTION;

      MERGE dbo.tParent AS p
      USING (SELECT tmp.[Name]
                  , tmp.[GMTDate]
                  , tmp.[GMTHour]
                  , tvp.[LOB]
                  , tmp.[LOBCheckSum]
                  , tmp.[Occurrences]
             FROM #TempTable AS tmp
             JOIN @TVP AS tvp 
             ON tmp.RowNumber = tvp.RowNumber) AS derived
      ON p.[Name] = derived.[Name]
      AND p.[LOBCheckSum] = derived.[LOBCheckSum]
      AND p.[GMTDate] = derived.[GMTDate]
      AND p.[GMTHour] = derived.[GMTHour]
      WHEN MATCHED THEN
          UPDATE SET [Occurrences] += derived.[Occurrences]
                   , [LastUpdateDate] = CAST(SYSDATETIME() AS DATE)
                   , [LastUpdateTime] = CAST(SYSDATETIME() AS TIME)     
      WHEN NOT MATCHED THEN
          INSERT ( [GMTDate] 
                 , [GMTHour] 
                 , [Name]
                 , [LOBCheckSum]
                 , [LOB] )
           VALUES
                 ( derived.[GMTDate]
                 , derived.[GMTHour]
                 , derived.[Name]
                 , derived.[LOBCheckSum]
                 , derived.[LOB] )
      OUTPUT Inserted.Id
           , Inserted.Name
           , Inserted.LOBCheckSum
           , Inserted.GMTDate
           , Inserted.GMTHour INTO #MergeMapper;

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
          map.Id
        , tvp.GMTDateTime
        , tvp.Value
        , tvp.Host
        , tvp.FloatingPoint
        , tvp.Boolean
        , tvp.Bytes
      FROM #MergeMapper map
      JOIN @TVP tvp
      ON map.[Name] = tvp.[Name]
      AND map.[LOBCheckSum] = tvp.[LOBCheckSum]
      AND map.[GMTDate] = CAST(tvp.GMTDateTime AS DATE)
      AND map.[GMTHour] = DATEPART(hour, tvp.GMTDateTime); 

      SET @InsertChildRows = @@ROWCOUNT;

      IF @LoggingLevel > 1
        SELECT @MergeParentRows AS [@MergeParentRows]
             , @MergeParent_ms AS [@MergeParent_ms]
             , @InsertChildRows AS [@InsertChildRows]
             , DATEDIFF(ms, @TimerDt, SYSDATETIME()) AS [@InsertChild_ms];

     COMMIT TRANSACTION;

     RETURN @InsertChildRows;


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

     IF XACT_STATE() < 0
       ROLLBACK TRANSACTION;

     RAISERROR ('dbo.pTVPMerge - Msg %d, Level %d, State %d, %s Line %d %s'
               ,16, 1, @Number, @Severity, @State, @Proc, @Line, @Message)

  END CATCH

END;

GO

-- this one uses an intermediate temp table that stablizes query perf somewhat
-- see below for one in that uses the TVP

IF OBJECT_ID (N'dbo.pTVPUpsert', N'P') IS NOT NULL
    DROP PROCEDURE dbo.pTVPUpsert;
GO
---------------------------------------------------------
-- add the rows in provided table valued parameter
---------------------------------------------------------
CREATE PROCEDURE dbo.pTVPUpsert
    ( @TVP TVPTableType READONLY 
    , @LoggingLevel TINYINT
    , @LoadMethod NVARCHAR(128) )
AS

BEGIN

  DECLARE @TimerDT DATETIME2;

  DECLARE @MergeParentRows INT
        , @MergeParent_ms INT    
        , @InsertChildRows INT
        , @InsertChild_ms INT;

  CREATE TABLE #UpsertMapper 
    ( Id BIGINT PRIMARY KEY
    , Name VARCHAR(50)
    , LOBCheckSum INT
    , GMTDate DATE
    , GMTHour INT
    , UNIQUE (Name, LOBCheckSum, GMTDate, GMTHour));

  CREATE TABLE #TempTable
    ( Name VARCHAR(50)
    , LOBCheckSum NVARCHAR(128)
    , GMTDate DATE
    , GMTHour INT 
    , Occurrences INT 
    , RowNumber INT
    , PRIMARY KEY (Name, LOBCheckSum, GMTDate, GMTHour)) 
       
  -- deduplicate before upsert
  INSERT #TempTable 
    ( Name
    , LOBCheckSum
    , GMTDate
    , GMTHour
    , Occurrences 
    , RowNumber)
  SELECT Name       
       , LOBCheckSum
       , CAST([GMTDateTime] AS DATE)
       , DATEPART(hour, [GMTDateTime])
       , COUNT(*)
       , MIN(RowNumber) 
  FROM @TVP
  GROUP BY [Name]
       , [LOBCheckSum]
       , CAST([GMTDateTime] AS DATE)
       , DATEPART(hour, [GMTDateTime]);
  
  IF @LoggingLevel > 1
    SET @TimerDT = SYSDATETIME();

  BEGIN TRY
 
    BEGIN TRANSACTION;

      UPDATE p 
      SET [Occurrences] += t.[Occurrences]
        , [LastUpdateDate] = CAST(SYSDATETIME() AS DATE)
        , [LastUpdateTime] = CAST(SYSDATETIME() AS TIME)     
      OUTPUT inserted.ID
           , inserted.Name
           , inserted.LOBCheckSum
           , inserted.GMTDate
           , inserted.GMTHour INTO #UpsertMapper
      FROM dbo.tParent AS p
      JOIN #TempTable AS t
      ON p.Name = t.Name
      AND p.LOBCheckSum = t.LOBCheckSum
      AND p.GMTDate = t.GMTDate
      AND p.GMTHour = t.GMTHour;

      SET @MergeParentRows = @@ROWCOUNT;

      INSERT dbo.tParent
        ( [Name]
        , [LOBCheckSum]
        , [GMTDate]
        , [GMTHour]
        , [LOB]
        , [Occurrences])
      OUTPUT inserted.ID
           , inserted.Name
           , inserted.LOBCheckSum
           , inserted.GMTDate
           , inserted.GMTHour INTO #UpsertMapper
      SELECT tmp.[Name]
           , tmp.[LOBCheckSum]
           , tmp.[GMTDate]
           , tmp.[GMTHour]
           , tvp.[LOB] AS [LOB] 
           , tmp.[Occurrences]  
      FROM #TempTable AS tmp
      LEFT JOIN dbo.tParent AS p
      ON p.Name = tmp.Name
      AND p.LOBCheckSum = tmp.LOBCheckSum
      AND p.GMTDate = tmp.GMTDate
      AND p.GMTHour = tmp.GMTHour
      JOIN @TVP AS tvp 
      ON tvp.RowNUmber = tmp.RowNumber
      WHERE p.[Id] IS NULL;

      SET @MergeParentRows += @@ROWCOUNT;

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
         map.Id
       , tvp.GMTDateTime
       , tvp.Value
       , tvp.Host
       , tvp.FloatingPoint
       , tvp.Boolean
       , tvp.Bytes
      FROM #UpsertMapper AS map
      JOIN @TVP AS tvp 
      ON map.[Name] = tvp.[Name]
      AND map.[LOBCheckSum] = tvp.[LOBCheckSum]
      AND map.[GMTDate] = CAST(tvp.GMTDateTime AS DATE)
      AND map.[GMTHour] = DATEPART(hour, tvp.GMTDateTime);

      SET @InsertChildRows = @@ROWCOUNT;
       
      IF @LoggingLevel > 1
        SELECT @MergeParentRows AS [@MergeParentRows]
             , @MergeParent_ms AS [@MergeParent_ms]
             , @InsertChildRows AS [@InsertChildRows]
             , DATEDIFF(ms, @TimerDt, SYSDATETIME()) AS [@InsertChild_ms];

    COMMIT TRANSACTION;
    
    RETURN @InsertChildRows;

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

     -- don't mess with anyone else's xacts
     IF XACT_STATE() <> 0
       ROLLBACK TRANSACTION;

     RAISERROR ('dbo.pTVPUpsert - Msg %d, Level %d, State %d, %s Line %d %s'
               ,16 ,1 ,@Number, @Severity, @State, @Proc, @Line, @Message);

  END CATCH

END;

GO

IF OBJECT_ID (N'dbo.StageMerge', N'P') IS NOT NULL
    DROP PROCEDURE dbo.StageMerge;

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
CREATE PROCEDURE dbo.StageMerge 
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

 CREATE TABLE #MergeMapper 
    ( Id BIGINT PRIMARY KEY
    , Name VARCHAR(50)
    , LOBCheckSum INT
    , GMTDate DATE
    , GMTHour INT
    , CONSTRAINT UQ_#MergeMapper
      UNIQUE (Name, LOBCheckSum, GMTDate, GMTHour));

 IF @LoggingLevel > 1
   SET @TimerDT = SYSDATETIME(); 

 -- stage table is a local temp table, for this reason this will 
 -- not work if the merge needs to be asynchronous from the import 
 CREATE TABLE #TempTable
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

 INSERT #TempTable
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

 CREATE NONCLUSTERED INDEX [ixn_#TempTable] ON #TempTable([Name],[LOBCheckSum],[GMTDateTime]); 

 IF @LoggingLevel > 1
   SET @CreateIndex_ms = DATEDIFF(ms, @TimerDt, SYSDATETIME());

 BEGIN TRY

   BEGIN TRANSACTION MergeTempTableToHierarchy;

     IF @LoggingLevel > 1
       SET @TimerDT = SYSDATETIME(); 

     MERGE dbo.tParent WITH(TABLOCK) AS p
     USING ( SELECT [Name]
                  , [LOBCheckSum]
                  , CAST([GMTDateTime] AS DATE) AS [GMTDate]
                  , DATEPART(hour, [GMTDateTime]) AS [GMTHour]
                  , MIN(LOB) AS [LOB] -- since LOBCheckSum determines equality no need to look in the LOB
                  , COUNT(*) AS [Occurrences]  
             FROM #TempTable
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
      OUTPUT Inserted.Id
           , Inserted.Name
           , Inserted.LOBCheckSum
           , Inserted.GMTDate
           , Inserted.GMTHour INTO #MergeMapper;

      SET @MergeParentRows = @@ROWCOUNT;

      IF @LoggingLevel > 1
        SET @MergeParent_ms = DATEDIFF(ms, @TimerDt, SYSDATETIME());

      IF @LoggingLevel > 1
        SET @TimerDT = SYSDATETIME(); 

      INSERT dbo.tChild WITH(TABLOCK)
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
      FROM #MergeMapper p
      JOIN #TempTable d
      ON p.[Name] = d.[Name]
      AND p.[LOBCheckSum] = d.[LOBCheckSum]
      AND p.[GMTDate] = CAST(d.GMTDateTime AS DATE)
      AND p.[GMTHour] = DATEPART(hour, d.GMTDateTime);

      SET @InsertChildRows = @@ROWCOUNT;
      
     IF @LoggingLevel > 1
       SET @InsertChild_ms = DATEDIFF(ms, @TimerDt, SYSDATETIME());
    
     COMMIT TRANSACTION MergeTempTableToHierarchy;
    
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

     -- don't mess with anyone else's xacts
     IF XACT_STATE() <> 0
       ROLLBACK TRANSACTION MergeTempTableToHierarchy;

     RAISERROR ('Msg %d, Level %d, State %d, %s Line %d %s'
               ,16, 1, @Number, @Severity, @State, @Proc, @Line, @Message)

  END CATCH

END;

GO

IF OBJECT_ID (N'dbo.StageUpsert', N'P') IS NOT NULL
    DROP PROCEDURE dbo.StageUpsert;

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
CREATE PROCEDURE dbo.StageUpsert 
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

  CREATE TABLE #UpsertMapper 
     ( Id BIGINT PRIMARY KEY
     , Name VARCHAR(50)
     , LOBCheckSum INT
     , GMTDate DATE
     , GMTHour INT
     , CONSTRAINT UQ_#UpsertMapper
       UNIQUE (Name, LOBCheckSum, GMTDate, GMTHour));

  IF @LoggingLevel > 1
    SET @TimerDT = SYSDATETIME(); 

  -- stage table is a local temp table, for this reason this will 
  -- not work if the merge needs to be asynchronous from the import 
  CREATE TABLE #TempTable
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

  INSERT #TempTable
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

  CREATE NONCLUSTERED INDEX [ixn_#TempTable] ON #TempTable([Name],[LOBCheckSum],[GMTDateTime]); 

  IF @LoggingLevel > 1
    SET @CreateIndex_ms = DATEDIFF(ms, @TimerDt, SYSDATETIME());

  BEGIN TRY

    BEGIN TRANSACTION UpsertTempTableToHierarchy;

      IF @LoggingLevel > 1
        SET @TimerDT = SYSDATETIME(); 

      UPDATE dbo.tParent WITH(TABLOCK)
      SET [Occurrences] = [Occurrences]
        , [LastUpdateDate] = CAST(SYSDATETIME() AS DATE)
        , [LastUpdateTime] = CAST(SYSDATETIME() AS TIME)     
      OUTPUT inserted.ID
           , inserted.Name
           , inserted.LOBCheckSum
           , inserted.GMTDate
           , inserted.GMTHour INTO #UpsertMapper
      SELECT t.[Name]
           , t.[LOBCheckSum]
           , CAST(t.[GMTDateTime] AS DATE) AS [GMTDate]
           , DATEPART(hour, t.[GMTDateTime]) AS [GMTHour]
           , COUNT(*) AS [Occurrences]  
      FROM #TempTable t
      JOIN dbo.tParent p WITH(TABLOCK)
      ON t.Name = p.Name
      AND t.LOBCheckSum = p.LOBCheckSum
      AND CAST(t.[GMTDateTime] AS DATE) = p.GMTDate
      AND DATEPART(hour, [GMTDateTime]) = p.GMTHour
      GROUP BY t.[NAME]
             , t.[LOBCheckSum]
             , CAST(t.[GMTDateTime] AS DATE)
             , DATEPART(hour, t.[GMTDateTime]);

      INSERT dbo.tParent WITH(TABLOCK) 
        ( [Name]
        , [LOBCheckSum]
        , [GMTDate]
        , [GMTHour]
        , [LOB]
        , [Occurrences])
      OUTPUT inserted.ID
           , inserted.Name
           , inserted.LOBCheckSum
           , inserted.GMTDate
           , inserted.GMTHour INTO #UpsertMapper
      SELECT t.[Name]
           , t.[LOBCheckSum]
           , CAST(t.[GMTDateTime] AS DATE) AS [GMTDate]
           , DATEPART(hour, t.[GMTDateTime]) AS [GMTHour]
           , MIN(t.LOB) AS [LOB] -- since LOBCheckSum determines equality no need to look in the LOB
           , COUNT(*) AS [Occurrences]  
      FROM #TempTable t
      LEFT JOIN dbo.tParent p WITH(TABLOCK)
      ON t.[Name] = p.[Name]
      AND t.[LOBCheckSum] = p.[LOBCheckSum]
      AND CAST(t.[GMTDateTime] AS DATE) = p.[GMTDate]
      AND DATEPART(hour, t.[GMTDateTime]) = p.[GMTHour]
      WHERE p.[Id] IS NULL
      GROUP BY t.[NAME]
             , t.[LOBCheckSum]
             , CAST(t.[GMTDateTime] AS DATE)
             , DATEPART(hour, t.[GMTDateTime]);

       SET @MergeParentRows = @@ROWCOUNT;

      IF @LoggingLevel > 1
        SET @MergeParent_ms = DATEDIFF(ms, @TimerDt, SYSDATETIME());

      IF @LoggingLevel > 1
        SET @TimerDT = SYSDATETIME(); 

       INSERT dbo.tChild WITH(TABLOCK) 
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
       FROM #UpsertMapper p
       JOIN #TempTable d
       ON p.[Name] = d.[Name]
       AND p.[LOBCheckSum] = d.[LOBCheckSum]
       AND p.[GMTDate] = CAST(d.GMTDateTime AS DATE)
       AND p.[GMTHour] = DATEPART(hour, d.GMTDateTime);

       SET @InsertChildRows = @@ROWCOUNT;
       
      IF @LoggingLevel > 1
        SET @InsertChild_ms = DATEDIFF(ms, @TimerDt, SYSDATETIME());
    
      COMMIT TRANSACTION UpsertTempTableToHierarchy;
    
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

     -- don't mess with anyone else's xacts
     IF XACT_STATE() <> 0
       ROLLBACK TRANSACTION UpsertTempTableToHierarchy;

     RAISERROR ('Msg %d, Level %d, State %d, %s Line %d %s'
               ,16, 1, @Number, @Severity, @State, @Proc, @Line, @Message)

  END CATCH

END;

GO

USE msdb;
GO

IF EXISTS (SELECT * FROM sys.routes WHERE name = 'target_route')
    DROP ROUTE target_route;
GO

use $(TARGETDB);
GO

IF OBJECT_ID('dbo.pTargetActivationProcedure','P') IS NOT NULL
    DROP PROCEDURE dbo.pTargetActivationProcedure;

GO
CREATE PROCEDURE dbo.pTargetActivationProcedure
AS
BEGIN
    SET NOCOUNT ON;
     
    -- Variable table for received messages.
    DECLARE @receive_table TABLE
        ( queuing_order BIGINT
        , conversation_handle UNIQUEIDENTIFIER
        , message_type_name SYSNAME
        , message_body VARBINARY(MAX));
    
    DECLARE @TableVariable TVPTableType;

    DECLARE @Datagram VARBINARY(MAX);
    DECLARE @datagram_type CHAR(3);
    
    -- Cursor for received message table.
    DECLARE message_cursor CURSOR LOCAL FORWARD_ONLY READ_ONLY
            FOR SELECT
            conversation_handle,
            message_type_name,
            message_body
            FROM @receive_table ORDER BY queuing_order;

    DECLARE @conversation_handle UNIQUEIDENTIFIER,
            @message_type_name SYSNAME,
            @message_body VARBINARY(MAX);
    
    -- Count processed messages.
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
 
    DECLARE @LoggingLevel TINYINT;
    DECLARE @LoadMethod NVARCHAR(128); 
    DECLARE @RowCount INT;
  
    -- target parameters.
    DECLARE @ReceiveMsgWaitTime CHAR(12);
    DECLARE @DialogRecycleMaxMessages BIGINT;
    
    DECLARE @StartDT DATETIME2;
    DECLARE @TimerDT DATETIME2;
    DECLARE @Receive_ms BIGINT;
    DECLARE @MessageToTablevariableRows BIGINT;
    DECLARE @MessageToTablevariable_ms BIGINT;
    DECLARE @TVPToTablevariableRows BIGINT;
    DECLARE @TVPToTablevariable_ms BIGINT;
    DECLARE @MergeParentRows BIGINT;
    DECLARE @MergeParent_ms BIGINT;
    DECLARE @InsertChildRows BIGINT;
    DECLARE @InsertChild_ms BIGINT;
    DECLARE @SumTVPToTablevariableRows BIGINT;
    DECLARE @SumTVPToTablevariable_ms BIGINT;
    DECLARE @SumMergeParentRows BIGINT;
    DECLARE @SumMergeParent_ms BIGINT;
    DECLARE @SumInsertChildRows BIGINT;
    DECLARE @SumInsertChild_ms BIGINT;
        
    SET @StartDt = SYSDATETIME();
    
    SET @message_counter = 0;
    SET @MessageToTablevariableRows = 0; 
    SET @MessageToTablevariable_ms = 0;
    SET @TVPToTablevariableRows = 0;
    SET @TVPToTablevariable_ms = 0;
    SET @MergeParentRows = 0;
    SET @MergeParent_ms = 0;
    SET @InsertChildRows = 0;
    SET @InsertChild_ms = 0;
    SET @SumTVPToTablevariableRows = 0;
    SET @SumTVPToTablevariable_ms = 0;
    SET @SumMergeParentRows = 0;
    SET @SumMergeParent_ms = 0;
    SET @SumInsertChildRows = 0;
    SET @SumInsertChild_ms = 0;
        
    SET @ReceiveMsgWaitTime = (SELECT ReceiveMsgWaitTime FROM dbo.tTransferParameters);
    SET @DialogRecycleMaxMessages = (SELECT DialogRecycleMaxMessages FROM dbo.tTransferParameters);
  
    SET @LoggingLevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);

    -- Receive messages for available conversation groups.
    BEGIN TRY
      WHILE (1=1)
      BEGIN
      
      BEGIN TRANSACTION;
    
         -- Receive max available messages into the table.
         -- Wait 5 seconds for messages.
         WAITFOR (
    		RECEIVE TOP(@DialogRecycleMaxMessages)
    		   queuing_order,		   
    		   conversation_handle,
    		   message_type_name,
    		   message_body
    		FROM target_queue
    		INTO @receive_table
         ), TIMEOUT 5000;

         IF @@ROWCOUNT = 0
         BEGIN
              COMMIT;
              BREAK;
         END
      
         -- Process the messages.
         OPEN message_cursor;
         WHILE (1=1)
         BEGIN
              FETCH NEXT FROM message_cursor
    	                INTO @conversation_handle,
    	                     @message_type_name,
    	                     @message_body;
    
              IF (@@FETCH_STATUS != 0) BREAK;

              -- Process a message.
              -- If an exception occurs, catch and attempt to recover.
              BEGIN TRY


                    IF @message_type_name = 'payload'
                    BEGIN

                        IF @LoggingLevel > 1
                          SET @TimerDT = SYSDATETIME();

                        DELETE @TableVariable; 

                        SELECT @LoadMethod = Row.value('@LoadMethod', 'nvarchar(128)')
                               --, @Count = Row.value('@Count','int') 
                        FROM (SELECT CAST(@message_body AS XML) AS [Payload]) AS [XMLRows]  
                        CROSS APPLY Payload.nodes('//rows') AS  [Rows]([Row]);

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
                        SELECT Row.value('@RowNumber', 'int') AS [RowNumber]
                             , Row.value('@Host','nvarchar(128)') AS [Host]
                             , Row.value('@GMTDateTime','datetime') AS [GMTDateTime]
                             , Row.value('@Name','varchar(50)') AS [Name]
                             , Row.value('@Value','int') AS [Value]
                             , Row.value('@FloatingPoint','real') AS [FloatingPoint]
                             , Row.value('@Boolean','bit') AS [Boolean]
                             , Row.value('@Bytes','varbinary(60)') AS [Bytes]
                             , Row.value('@LOB','nvarchar(max)') AS [LOB]
                             , Row.value('@LOBCheckSum','int') AS [LOBCheckSum]
                        FROM (SELECT CAST(@message_body AS XML) AS [Payload]) AS [XMLRows]  
                        CROSS APPLY Payload.nodes('//rows/row') AS  [Rows]([Row]);
  
                        SET @MessageToTableVariableRows += @@ROWCOUNT; 
                                                
                        IF @LoggingLevel > 1
                          SET @MessageToTableVariable_ms += DATEDIFF(ms, @TimerDT, SYSDATETIME());

                        --EXEC dbo.pTVPMerge @TableVariable
                        EXEC dbo.pTVPUpsert @TableVariable
                                         , @LoggingLevel
                                         , @LoadMethod    
                                         , @TVPToTableVariableRows OUTPUT
                                         , @TVPToTableVariable_ms OUTPUT
                                         , @MergeParentRows OUTPUT
                                         , @MergeParent_ms OUTPUT
                                         , @InsertChildRows OUTPUT
                                         , @InsertChild_ms OUTPUT;
                     
                        --IF @InsertChildRows = 1
                        --  SET @LoadMethod = 'RowSendXML'
                        --ELSE  
                        --  SET @LoadMethod = 'TVPSendXML';

                        SET @SumTVPToTableVariableRows += @TVPToTableVariableRows;
                        SET @SumTVPToTableVariable_ms  += @TVPToTableVariable_ms;
                        SET @SumMergeParentRows  += @MergeParentRows;
                        SET @SumMergeParent_ms += @MergeParent_ms;
                        SET @SumInsertChildRows += @InsertChildRows;
                        SET @SumInsertChild_ms += @InsertChild_ms;
                               
                        WAITFOR DELAY @ReceiveMsgWaitTime; 

                       SET @message_counter += 1;
                    END
                    ELSE IF @message_type_name = 'datagram'
                    BEGIN

                        IF @LoggingLevel > 1
                          SET @TimerDT = SYSDATETIME();
                          
                        SET @datagram_type = CAST(SUBSTRING(@message_body, 1, 3) AS CHAR(3));
                        SET @Datagram = CAST(SUBSTRING(@message_body, 4, DATALENGTH(@message_body)) AS VARBINARY(MAX))

                        DELETE @TableVariable; 

                        IF @datagram_type = 'row'
                          BEGIN
                            SET @LoadMethod = 'RowSendBinary'

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
                            FROM dbo.fBinaryUnmarshalRow(@Datagram);

                          END

                        ELSE IF @datagram_type = 'TVP'
               
                          BEGIN 
                          
                            SET @LoadMethod = 'TVPSendBinary'

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
                            FROM dbo.fBinaryUnmarshalTVP(@Datagram);
  
                          END;
                          
                        SET @MessageToTableVariableRows += @@ROWCOUNT; 
                                                
                        IF @LoggingLevel > 1
                          SET @MessageToTableVariable_ms += DATEDIFF(ms, @TimerDT, SYSDATETIME());
                                                    
                        --EXEC dbo.pTVPMerge @TableVariable
                        EXEC dbo.pTVPUpsert @TableVariable
                                         , @LoggingLevel
                                         , @LoadMethod    
                                         , @TVPToTableVariableRows OUTPUT
                                         , @TVPToTableVariable_ms OUTPUT
                                         , @MergeParentRows OUTPUT
                                         , @MergeParent_ms OUTPUT
                                         , @InsertChildRows OUTPUT
                                         , @InsertChild_ms OUTPUT;
                        
                        WAITFOR DELAY @ReceiveMsgWaitTime; 

                        SET @message_counter = @message_counter + 1;
                    END
                    ELSE IF @message_type_name = 'end_of_stream'
                    BEGIN
                         -- Initiator is signaling end of message stream: end the dialog.
                         END CONVERSATION @conversation_handle;
                    END
                    ELSE IF @message_type_name = 'http://schemas.microsoft.com/SQL/ServiceBroker/Error'
                    BEGIN
                         -- If the message_type_name indicates that the message is an error,
                         -- record the error and end the conversation.
                         WITH XMLNAMESPACES ('http://schemas.microsoft.com/SQL/ServiceBroker/Error' AS ssb)
                         SELECT
                         @error_number = CAST(@message_body AS XML).value('(//ssb:Error/ssb:Code)[1]', 'INT'),
                         @error_message = CAST(@message_body AS XML).value('(//ssb:Error/ssb:Description)[1]', 'VARCHAR(4000)');
                         INSERT INTO dbo.tTargetProcessingErrors 
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
                               , NULL
                               , NULL
                               , 0
                               , CAST(@message_body AS XML));
        	               END CONVERSATION @conversation_handle;
                    END

              END TRY
              BEGIN CATCH
                   SET @error_number = ERROR_NUMBER();
                   SET @error_message = ERROR_MESSAGE();
                   SET @error_severity = ERROR_SEVERITY();
                   SET @error_state = ERROR_STATE();
                   SET @error_procedure = ERROR_PROCEDURE();
                   SET @error_line = ERROR_LINE();
            
                   IF XACT_STATE() = -1
                   BEGIN
                        -- The transaction is doomed. Only rollback possible.
                        -- This could disable the queue if done 5 times consecutively!
                        ROLLBACK TRANSACTION;
            
                        -- Record the error.
                        BEGIN TRANSACTION;
                        INSERT INTO tTargetProcessingErrors 
                           ( [error_conversation]
                           , [error_number]
                           , [error_message]
                           , [error_severity]
                           , [error_state]
                           , [error_procedure]
                           , [error_line]
                           , [doomed_transaction] 
                           , [error_xml] )
                        VALUES( NULL
                              , @error_number
                              , @error_message
                              , @error_severity
                              , @error_state
                              , @error_procedure
                              , @error_line
                              , 1
                              , NULL);
                        COMMIT;

                        -- For this level of error, it is best to exit the proc
                        -- and give the queue monitor control.
                        -- Breaking to the outer catch will accomplish this.
                        RAISERROR ('Message processing error #%d', 16, 1, @error_number);
                   END
                   ELSE IF XACT_STATE() = 1
                   BEGIN
                        -- Record error and continue processing messages.
                        -- Failing message could also be put aside for later processing here.
                        -- Otherwise it will be discarded.
                        INSERT INTO tTargetProcessingErrors 
                           ( [error_conversation]
                           , [error_number]
                           , [error_message]
                           , [error_severity]
                           , [error_state]
                           , [error_procedure]
                           , [error_line]
                           , [doomed_transaction] 
                           , [error_xml] )                        
                        VALUES( NULL
                              , @error_number
                              , @error_message
                              , @error_severity
                              , @error_state
                              , @error_procedure
                              , @error_line
                              , 0
                              , NULL);
                   END

              END CATCH
         END
         CLOSE message_cursor;
         DELETE @receive_table;
         COMMIT;

      END

    END TRY
    BEGIN CATCH
    
       -- Process the error and exit the proc to give the queue monitor control
       SET @error_conversation = @conversation_handle
       SET @error_number = ERROR_NUMBER();
       SET @error_message = ERROR_MESSAGE();
       SET @error_severity = ERROR_SEVERITY();
       SET @error_state = ERROR_STATE();
       SET @error_procedure = ERROR_PROCEDURE();
       SET @doomed_transaction = XACT_STATE();
       SET @error_line = ERROR_LINE();

       IF @doomed_transaction = -1
           BEGIN
                -- The transaction is doomed. Only rollback possible.
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
       VALUES( NULL
             , @error_number
             , @error_message
             , @error_severity
             , @error_state
             , @error_procedure
             , @error_line
             , @doomed_transaction
             , @error_xml);
       COMMIT;

    END CATCH

    -- Increment processed message counter.
    BEGIN TRANSACTION;
    DECLARE @counter BIGINT;
    SET @counter = (SELECT TOP(1) counter FROM tTargetMessageCounter);
    SET @counter = @counter + @message_counter;
    UPDATE tTargetMessageCounter SET counter = @counter;
    COMMIT;
    
    SET @Receive_ms = DATEDIFF(ms, @StartDt, SYSDATETIME()); 

    -- do something with the metadata  
    IF @LoggingLevel > 0
     BEGIN
       BEGIN TRANSACTION;
            
       DECLARE @ActivityHistoryID BIGINT;
       SET @ActivityHistoryID = (SELECT MAX(a.Id) 
                                 FROM dbo.tActivityHistory a
                                 LEFT JOIN dbo.tReceiveHistory r
                                 on a.Id = r.ActivityHistoryId
                                 WHERE a.LoadMethod = @LoadMethod
                                 AND a.StartDt < @StartDt
                                 AND r.ActivityHistoryId IS NULL);      

       INSERT dbo.tReceiveHistory
          ( ActivityHistoryId
          , LoadMethod	
          , Receive_ms
          , MessageToTablevariableRows
          , MessageToTablevariable_ms 
          , TVPToTableVariableRows
          , TVPToTableVariable_ms
          , MergeParentRows
          , MergeParent_ms
          , InsertChildRows
          , InsertChild_ms ) 
        VALUES
         ( @ActivityHistoryId
         , @LoadMethod
         , @Receive_ms
         , @MessageToTablevariableRows
         , @MessageToTablevariable_ms 
         , @TVPToTableVariableRows
         , @TVPToTableVariable_ms
         , @MergeParentRows
         , @MergeParent_ms
         , @InsertChildRows
         , @InsertChild_ms )

       COMMIT;
     END
END;

GO

-- Drop & Create Service Broker objects in dependency order
IF EXISTS (SELECT * FROM sys.services WHERE name = 'initiator_service')
    DROP SERVICE initiator_service;
GO        
IF EXISTS (SELECT * FROM sys.services WHERE name = 'target_service')
    DROP SERVICE target_service;
GO        
IF EXISTS (SELECT * FROM sys.service_queues WHERE name = 'target_queue')
    DROP QUEUE target_queue;
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
IF EXISTS (SELECT * FROM sys.routes WHERE name = 'initiator_route')
    DROP SERVICE initiator_route;
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
--   Target Specific
--------------------------- 
BEGIN
 -- Create the target queue with specified number of activated procedures.
 DECLARE @query NVARCHAR(500)
 SELECT @query = N'CREATE QUEUE target_queue
   WITH ACTIVATION (
   STATUS = ON,
   MAX_QUEUE_READERS = ' + CAST(MaxQueueReaders AS NVARCHAR(10)) + N',
   PROCEDURE_NAME = dbo.pTargetActivationProcedure,
   EXECUTE AS OWNER)'
 FROM dbo.tTransferParameters;
 EXEC sp_executesql @query;

 IF @@ERROR <> 0
  -- level 10 returns only message line no error,state,level
  -- the actual error will be displayed above this message
  RAISERROR('Warning: Not all target side Service Broker have been created.',10,1)
 ElSE
  BEGIN
   -- Create target service.
   CREATE SERVICE target_service 
   ON QUEUE target_queue ( binary_contract
                         , xml_contract);

   -- Any valid user in the database can send messages on this service.
   GRANT SEND ON SERVICE::target_service TO BrokerSenderRole;
 END;

END;

-----------------------------------------------------------------------------
--  recreate Initiator service if the initiator queue exists in the local db  
----------------------------------------------------------------------------- 
IF EXISTS (SELECT * FROM sys.service_queues WHERE name = 'initiator_queue')
 BEGIN
   -- Create initiator service.
   CREATE SERVICE initiator_service 
   ON QUEUE initiator_queue ( binary_contract
                            , xml_contract);

   -- Any valid user in the database can send messages on this service.
   GRANT SEND ON SERVICE::initiator_service TO BrokerSenderRole;
 END;

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
AND DB_ID('$(TARGETDB)') IS NOT NULL 
AND DB_ID('$(INITIATORDB)') IS NULL
  CREATE ENDPOINT ServiceBrokerEndpoint
  STATE = STARTED
  AS TCP (LISTENER_PORT = $(TARGETPORT))
  FOR SERVICE_BROKER (AUTHENTICATION = Windows, ENCRYPTION = $(ENCRYPTION));

GO

DECLARE @DomainName NVARCHAR(128)
      , @InitiatorHost NVARCHAR(128)
      , @TargetHost NVARCHAR(128)
      , @TargetLogin NVARCHAR(256);
SET @DomainName = '$(DOMAINNAME)';
SET @TargetHost = '$(TARGETHOST)';
                          
IF DB_ID('$(TARGETDB)') IS NOT NULL
AND SERVERPROPERTY('MachineName') = '$(INITIATORHOST)'
AND SERVERPROPERTY('MachineName') <> '$(TARGETHOST)' 
  BEGIN
    SET @TargetLogin = @DomainName 
                        + CASE WHEN @DomainName = '' 
                               THEN ''
                               ELSE '\'
                               END
                         + @TargetHost 
                         + '$';
    IF NOT EXISTS (SELECT * FROM sys.syslogins WHERE name = @TargetLogin)
      EXEC('CREATE LOGIN ' + @TargetLogin + ' FROM Windows');
    EXEC('GRANT CONNECT ON ENDPOINT::ServiceBrokerEndpoint TO ' + @TargetLogin);
  END  

-- only need routes if initiator and target not on same SQL instancTDB)
-- Create a route to the initiator service.
IF DB_NAME() = '$(TARGETDB)'
 CREATE ROUTE initiator_route
	WITH SERVICE_NAME = 'initiator_service',
	ADDRESS = 'tcp://$(INITIATORHOST):$(INITIATORPORT)';

GO

-- In msdb, create an incoming route to the target service.
USE msdb;
GO
IF DB_ID('$(TARGETDB)') IS NOT NULL
  CREATE ROUTE target_route
	 WITH SERVICE_NAME = 'target_service',
	 ADDRESS = 'local';
GO


USE master;
 
GO 

