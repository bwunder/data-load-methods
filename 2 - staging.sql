-- SQLCMD mode substitution variables - always wrap in double quotes

:SETVAR TARGETDB "target"        -- must match target name in 1 - a - CREATE target.sql                                   
:SETVAR STAGINGDB "staging"                                                                                               
:SETVAR DATAPATH "C:\Program Files\Microsoft SQL Server\MSSQL10_50.ATER2\MSSQL\DATA" -- R2 "G:\MSSQL10_50.R2\MSSQL\DATA\" 
:SETVAR LOGPATH  "C:\Program Files\Microsoft SQL Server\MSSQL10_50.ATER2\MSSQL\DATA" -- R2 "G:\MSSQL10_50.R2\MSSQL\DATA\" 

SET NOCOUNT ON;
GO
USE master;
GO
------------------------------------------------------------
-- staging
------------------------------------------------------------
-- staging not used by synchronous or brokered load methods 
-- created on same SQL Instance as the target db
------------------------------------------------------------
-- contents     
--                         
-- staging DDL
-- dbo.TargetUpsert         
-- dbo.BulkLoadUpsert
--
-- dbo.ViewUpsert is in the GenerateABTableProc.sql       
------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '$(STAGINGDB)')
  BEGIN
    
    CREATE DATABASE $(STAGINGDB)
    ON ( NAME = $(STAGINGDB),
         FILENAME = '$(DATAPATH)\$(STAGINGDB).mdf',
         SIZE = 2000MB,
         MAXSIZE = 5000MB,
         FILEGROWTH = 50MB )
    LOG ON ( NAME = $(STAGINGDB)_log,
             FILENAME = '$(LOGPATH)\$(STAGINGDB)_log.ldf',
             SIZE = 500MB,
             MAXSIZE = 1000MB,
             FILEGROWTH = 50MB);

    ALTER DATABASE $(STAGINGDB)
    SET RECOVERY SIMPLE;

    IF EXISTS (SELECT * FROM sys.databases 
               WHERE name = '$(TARGETDB)'
               AND is_read_committed_snapshot_on = 1) 
      ALTER DATABASE $(STAGINGDB)
      SET READ_COMMITTED_SNAPSHOT ON;

  END;
  
GO

USE $(STAGINGDB);

GO

IF EXISTS(SELECT * FROM sys.databases
          WHERE database_id = DB_ID()
          AND name = '$(STAGINGDB)'
          AND owner_sid <> 0x01)
  EXEC sp_changedbowner 'sa'

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

IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'tTransferParameters')
  DROP SYNONYM [dbo].[tTransferParameters]

IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'tActivityHistory')
  DROP SYNONYM [dbo].[tActivityHistory]
GO

IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'tReceiveHistory')
  DROP SYNONYM [dbo].[tReceiveHistory]
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

IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'pGetActivityForLoadMethod')
  DROP SYNONYM [dbo].[pGetActivityForLoadMethod]
GO

IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'pGetQueueSizes')
  DROP SYNONYM [dbo].[pGetQueueSizes]
GO

IF '$(TARGETDB)' <> '$(STAGINGDB)'
  CREATE SYNONYM [dbo].[tTransferParameters] FOR [$(TARGETDB)].[dbo].[tTransferParameters];
  CREATE SYNONYM [dbo].[tActivityHistory] FOR [$(TARGETDB)].[dbo].[tActivityHistory];
  CREATE SYNONYM [dbo].[tReceiveHistory] FOR [$(TARGETDB)].[dbo].[tReceiveHistory];
  CREATE SYNONYM [dbo].[pRowUpsert] FOR [$(TARGETDB)].[dbo].[pRowUpsert]
  CREATE SYNONYM [dbo].[fGetTableVariable] FOR [$(TARGETDB)].[dbo].[fGetTableVariable];
  CREATE SYNONYM [dbo].[pGetIndexDetails] FOR [$(TARGETDB)].[dbo].[pGetIndexDetails];
  CREATE SYNONYM [dbo].[pGetQueueSizes] FOR [$(TARGETDB)].[dbo].[pGetQueueSizes];
  CREATE SYNONYM [dbo].[pGetActivityForLoadMethod] FOR [$(TARGETDB)].[dbo].[pGetActivityForLoadMethod];
GO

  IF OBJECT_ID('dbo.pGetStagingRowcounts','P') IS NOT NULL
    DROP PROCEDURE dbo.pGetStagingRowcounts;

GO
 CREATE PROCEDURE dbo.pGetStagingRowcounts
 AS
 BEGIN
      SELECT o.name
           , p.rows
           , v.VIEW_NAME
           , p.partition_number
      FROM sys.objects AS o
      JOIN sys.partitions AS p 
      ON p.object_id = o.object_id
      LEFT JOIN INFORMATION_SCHEMA.VIEW_TABLE_USAGE AS v
      ON o.name = v.TABLE_NAME
      WHERE o.Type = 'U' 
      AND p.index_id < 2;
 END;
GO


IF OBJECT_ID('dbo.StagedData','U') IS NOT NULL
  DROP TABLE dbo.StagedData;

GO
CREATE TABLE dbo.StagedData
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
 
GO

GRANT SELECT, INSERT, DELETE on dbo.StagedData  to Public 

GO

IF OBJECT_ID('dbo.vSourceDataReader','V') IS NOT NULL
  DROP VIEW dbo.vSourceDataReader;

GO

IF OBJECT_ID('dbo.vSourceDataWriter','V') IS NOT NULL
  DROP VIEW dbo.vSourceDataWriter;

GO

IF OBJECT_ID('dbo.SourceDataA','U') IS NOT NULL
  DROP TABLE dbo.SourceDataA;

CREATE TABLE dbo.SourceDataA
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
 
GO
 
 CREATE VIEW dbo.vSourceDataWriter 
 WITH SCHEMABINDING
 AS
  -- minimal logging is a goal, table lock is required. 
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
    FROM dbo.SourceDataA;  

GO

IF OBJECT_ID('dbo.SourceDataB','U') IS NOT NULL
  DROP TABLE dbo.SourceDataB;

CREATE TABLE dbo.SourceDataB
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
 
GO
  
 CREATE VIEW dbo.vSourceDataReader
 WITH SCHEMABINDING
 AS
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
    FROM dbo.SourceDataB;

GO



IF OBJECT_ID (N'dbo.ABViewMerge', N'P') IS NOT NULL
    DROP PROCEDURE dbo.ABViewMerge;
GO
-----------------------------------------------------
-- the source data is added to an unindexed heap as
-- the data becomes available. Periodically a
-- process replaces the current heap with an empty
-- one then indexes and loads the rows. 
-- Reader view is used to load the hierarchy
-- Writer view recieves rows from n sources at any 
-- granularity, may need a master baker
---------------------------------------------------- 
CREATE PROCEDURE dbo.ABViewMerge
AS
BEGIN

  DECLARE @LoggingLevel TINYINT
        , @StartDt DATETIME2
        , @TimerDt DATETIME2
        , @MergeParent_ms BIGINT
        , @MergeParentRows BIGINT
        , @InsertChild_ms BIGINT
        , @InsertChildRows BIGINT;

  CREATE TABLE #MergeMapper 
    ( Id BIGINT PRIMARY KEY
    , Name VARCHAR(50)
    , LOBCheckSum INT
    , GMTDate DATE
    , GMTHour INT
    , UNIQUE (LOBCheckSum, Name, GMTDate, GMTHour));

  SET @StartDt = SYSDATETIME(); 
  SET @Logginglevel = (SELECT LoggingLevel FROM dbo.tTransferParameters)

    BEGIN TRY
    
      IF @LoggingLevel > 0
        SET @TimerDT = SYSDATETIME(); 

      BEGIN TRANSACTION;

        -- LOBCheckSum determines uniqueness, no need to look in the LOB but MIN is a crappy way
        MERGE $(TARGETDB).dbo.tParent AS p
        USING ( SELECT [Name]
                     , [LOBCheckSum]
                     , CAST([GMTDateTime] AS DATE) AS [GMTDate]
                     , DATEPART(hour, [GMTDateTime]) AS [GMTHour]
                     , MIN(LOB) AS [LOB] 
                     , COUNT(*) AS [Occurrences]  
                FROM dbo.vSourceDataReader
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

        IF @LoggingLevel > 0
          SET @MergeParent_Ms = DATEDIFF(ms, @TimerDT, SYSDATETIME()); 

        IF @LoggingLevel > 0
          SET @TimerDT = SYSDATETIME(); 

        INSERT $(TARGETDB).dbo.tChild 
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
        JOIN dbo.vSourceDataReader d
        ON p.[Name] = d.[Name]
        AND p.[LOBCheckSum] = d.[LOBCheckSum]
        AND p.[GMTDate] = CAST(d.GMTDateTime AS DATE)
        AND p.[GMTHour] = DATEPART(hour, d.GMTDateTime);
           
        SET @InsertChildRows = @@ROWCOUNT;

        IF @LoggingLevel > 0
          SET @InsertChild_ms = DATEDIFF(ms, @TimerDT, SYSDATETIME());  
   
      -- leave the data in the staging table until next import 
      -- large received rowsets may need and index on the mapper      

      COMMIT TRANSACTION; 

      IF @LoggingLevel > 0
        INSERT dbo.tReceiveHistory
          ( LoadMethod	
          , Receive_ms
          , MergeParentRows
          , MergeParent_ms 
          , InsertChildRows
          , InsertChild_ms ) 
        SELECT OBJECT_NAME(@@PROCID)       
             + CASE WHEN d.is_read_committed_snapshot_on = 0 
                    THEN 'LCK' ELSE 'VER' END
             + CASE WHEN i.allow_row_locks = 1
                    THEN 'ROW' ELSE 'PAG' END
             + c.Concurrency                     
             , DATEDIFF(ms, @StartDT, SYSDATETIME()) 
             , @MergeParentRows
             , @MergeParent_ms 
             , @InsertChildRows
             , @InsertChild_ms
        FROM sys.databases AS d
        CROSS APPLY (SELECT allow_row_locks 
                     FROM sys.indexes
                     WHERE OBJECT_NAME(object_id) = 'SourceDataA'
                     AND index_id < 2) AS i
        CROSS APPLY (SELECT ISNULL(MAX(REPLACE(name,'ABLoad','')), '-99') AS Concurrency
                     FROM msdb.dbo.sysjobs
                     WHERE name like 'ABLoad%'
                     AND enabled = 1) AS c 
        WHERE d.name = DB_NAME();

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
         ROLLBACK TRANSACTION;

       RAISERROR ('Msg %d, Level %d, State %d, %s Line %d %s'
                 ,16 ,1 ,@Number, @Severity, @State, @Proc, @Line, @Message)
         
     END CATCH;   
            
END;

GO

IF OBJECT_ID (N'dbo.ABViewUpsert', N'P') IS NOT NULL
    DROP PROCEDURE dbo.ABViewUpsert;
GO
CREATE PROCEDURE dbo.ABViewUpsert
AS
BEGIN

  DECLARE @StartDT DATETIME2
        , @TimerDT DATETIME2
        , @StageParentRows INT
        , @MergeParentRows INT
        , @MergeParent_ms INT    
        , @InsertChildRows INT
        , @InsertChild_ms INT
        , @LoggingLevel TINYINT;

  SET @StartDt = SYSDATETIME(); 
  SET @Logginglevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);

 -- pass surrogate keys to child
  CREATE TABLE #Mapper 
    ( Id INT PRIMARY KEY
    , Name VARCHAR(50)
    , LOBCheckSum INT
    , GMTDate DATE
    , GMTHour INT
    , UNIQUE (Name, LOBCheckSum, GMTDate, GMTHour) );

  -- data is static so dedup before txn 
  CREATE TABLE #Stage
    ( Id INT IDENTITY(1,1) PRIMARY KEY
    , Name VARCHAR(50)
    , LOBCheckSum NVARCHAR(128)
    , GMTDate DATE
    , GMTHour INT 
    , Occurrences INT 
    , UNIQUE (GMTDate, GMTHour, LOBCheckSum, Name));
    
  INSERT #Stage 
    ( Name
    , LOBCheckSum
    , GMTDate
    , GMTHour
    , Occurrences )
  SELECT Name       
       , LOBCheckSum
       , CAST([GMTDateTime] AS DATE)
       , DATEPART(hour, [GMTDateTime])
       , COUNT(*)
  FROM dbo.vSourceDataReader
  GROUP BY [Name]
       , [LOBCheckSum]
       , CAST([GMTDateTime] AS DATE)
       , DATEPART(hour, [GMTDateTime]); 
         
  SET @StageParentRows = @@ROWCOUNT;

  IF @LoggingLevel > 0
    SET @TimerDT = SYSDATETIME();

  BEGIN TRY
 
    BEGIN TRANSACTION;

      -- update existing (no need to handle the LOB)
      UPDATE p 
      SET [Occurrences] += s.[Occurrences]
        , [LastUpdateDate] = CAST(SYSDATETIME() AS DATE)
        , [LastUpdateTime] = CAST(SYSDATETIME() AS TIME)     
      OUTPUT inserted.Id
           , inserted.Name
           , inserted.LOBCheckSum
           , inserted.GMTDate
           , inserted.GMTHour INTO #Mapper
      FROM $(TARGETDB).dbo.tParent AS p
      JOIN #Stage AS s
      ON p.Name = s.Name
      AND p.LOBCheckSum = s.LOBCheckSum
      AND p.GMTDate = s.GMTDate
      AND p.GMTHour = s.GMTHour;

      SET @MergeParentRows = @@ROWCOUNT;

      -- add new, need LOB and want to minimize potential race condition 
      INSERT INTO $(TARGETDB).dbo.tParent
        ( [Name]
        , [LOBCheckSum]
        , [GMTDate]
        , [GMTHour]
        , [LOB]
        , [Occurrences])
      OUTPUT inserted.Id
           , inserted.Name
           , inserted.LOBCheckSum
           , inserted.GMTDate
           , inserted.GMTHour INTO #Mapper
      SELECT s.[Name]
           , s.[LOBCheckSum]
           , s.[GMTDate]
           , s.[GMTHour]
           , v.[LOB] AS [LOB] 
           , s.[Occurrences]  
      FROM #Stage AS s
      OUTER APPLY (SELECT TOP(1) LOB 
                   FROM dbo.vSourceDataReader 
                   WHERE Name = s.Name
                   AND LOBCheckSum = s.LOBCheckSum
                   AND GMTDate = s.GMTDate
                   AND GMTHour = s.GMTHour ) AS v
      LEFT JOIN $(TARGETDB).dbo.tParent AS p 
      ON p.Name = s.Name
      AND p.LOBCheckSum = s.LOBCheckSum
      AND p.GMTDate = s.GMTDate
      AND p.GMTHour = s.GMTHour
      WHERE p.Id IS NULL; 
      
      SET @MergeParentRows += @@ROWCOUNT;

      IF @StageParentRows > @MergeParentRows
        BEGIN  
          -- another processes may have added row(s) between upsert and insert 
          UPDATE p 
          SET [Occurrences] += s.[Occurrences]
            , [LastUpdateDate] = CAST(SYSDATETIME() AS DATE)
            , [LastUpdateTime] = CAST(SYSDATETIME() AS TIME)     
          OUTPUT inserted.Id
               , inserted.Name
               , inserted.LOBCheckSum
               , inserted.GMTDate
               , inserted.GMTHour INTO #Mapper
          FROM $(TARGETDB).dbo.tParent AS p
          JOIN #Stage AS s
          ON p.Name = s.Name
          AND p.LOBCheckSum = s.LOBCheckSum
          AND p.GMTDate = s.GMTDate
          AND p.GMTHour = s.GMTHour
          LEFT JOIN #Mapper map
          ON map.Id = p.Id
          WHERE map.Id IS NULL;        
          
          SET @MergeParentRows += @@ROWCOUNT;
          IF @StageParentRows > @MergeParentRows  
            RAISERROR('Staged parent rows %d, Merged parent rows %d, throwing some rows on the floor',10,1,@StageParentRows,@MergeParentRows);
          
        END;

      IF @LoggingLevel > 0
        SET @MergeParent_ms = DATEDIFF(ms, @TimerDt, SYSDATETIME());

      IF @LoggingLevel > 0
        SET @TimerDT = SYSDATETIME(); 

      INSERT $(TARGETDB).dbo.tChild 
       ( ParentId 
       , GMTDateTime
       , Value
       , Host
       , FloatingPoint
       , Boolean
       , Bytes )
      SELECT m.Id
           , v.GMTDateTime
           , v.Value
           , v.Host
           , v.FloatingPoint
           , v.Boolean
           , v.Bytes
      FROM #Mapper AS m
      JOIN dbo.vSourceDataReader AS v 
      ON m.[Name] = v.[Name]
      AND m.[LOBCheckSum] = v.[LOBCheckSum]
      AND m.[GMTDate] = CAST(v.GMTDateTime AS DATE)
      AND m.[GMTHour] = DATEPART(hour, v.GMTDateTime);

      SET @InsertChildRows = @@ROWCOUNT;
       
      IF @LoggingLevel > 0
        SET @InsertChild_ms = DATEDIFF(ms, @TimerDt, SYSDATETIME());

      IF @LoggingLevel > 0
        INSERT dbo.tReceiveHistory
            ( LoadMethod	
            , Receive_ms
            , MergeParentRows
            , MergeParent_ms 
            , InsertChildRows
            , InsertChild_ms ) 
        SELECT OBJECT_NAME(@@PROCID)       
             + CASE WHEN d.is_read_committed_snapshot_on = 0 
                    THEN 'LCK' ELSE 'VER' END
             + CASE WHEN i.allow_row_locks = 1
                    THEN 'ROW' ELSE 'PAG' END
             + c.Concurrency                     
             , DATEDIFF(ms, @StartDT, SYSDATETIME()) 
             , @MergeParentRows
             , @MergeParent_ms 
             , @InsertChildRows
             , @InsertChild_ms
        FROM sys.databases AS d
        CROSS APPLY (SELECT allow_row_locks 
                     FROM sys.indexes
                     WHERE OBJECT_NAME(object_id) = 'SourceDataA'
                     AND index_id < 2) AS i
        CROSS APPLY (SELECT ISNULL(MAX(REPLACE(name,'ABLoad','')), '-99') AS Concurrency
                     FROM msdb.dbo.sysjobs
                     WHERE name like 'ABLoad%'
                     AND enabled = 1) AS c 
        WHERE d.name = DB_NAME();


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

     -- don't mess with anyone else's xacts
     IF XACT_STATE() <> 0
       ROLLBACK TRANSACTION;

     RAISERROR ('Msg %d, Level %d, State %d, %s Line %d %s'
               ,16 ,1 ,@Number, @Severity, @State, @Proc, @Line, @Message);

  END CATCH

END;

GO

IF OBJECT_ID (N'dbo.ABChangeView', N'P') IS NOT NULL
    DROP PROCEDURE dbo.ABChangeView;
GO
-----------------------------------------------------
-- the source data is added to an unindexed heap as
-- the data becomes available. 
-- Periodically a process replaces the current heap 
-- with empty one and loads the collected rows. 
---------------------------------------------------- 
CREATE PROCEDURE dbo.ABChangeView
AS
BEGIN

  DECLARE @LoggingLevel TINYINT
        , @IndexName NVARCHAR(128)
        , @StartDt DATETIME2
        , @TimerDt DATETIME2
        , @WasReaderTable NVARCHAR(128)
        , @WasWriterTable NVARCHAR(128)
        , @ReaderViewAlterScript NVARCHAR(1000)
        , @WriterViewAlterScript NVARCHAR(1000)
        , @SQLStr NVARCHAR(MAX)
        , @SwapViews_ms BIGINT
        , @IndexHeap_ms BIGINT
        , @ResourceLocker INT
        , @AllowRowLocks NVARCHAR(3)
        , @AllowPageLocks NVARCHAR(3);

  SET @Logginglevel = (SELECT LoggingLevel FROM dbo.tTransferParameters)
  SET @IndexName = 'ixn_SourceDataAB__Name__LOBSize__GMTDateTime' 

  IF @LoggingLevel > 1
    SET @StartDt = SYSDATETIME(); 

  -- swap the views to the opposite tables
  SET @WasWriterTable = (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEW_TABLE_USAGE WHERE VIEW_NAME = 'vSourceDataWriter');  
  SET @WasReaderTable = (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEW_TABLE_USAGE WHERE VIEW_NAME = 'vSourceDataReader');  

  -- validation - 
  IF NOT (   (@WasReaderTable = 'SourceDataA'
              AND @WasWriterTable = 'SourceDataB')
          OR (@WasReaderTable = 'SourceDataB'
              AND @WasWriterTable = 'SourceDataA')
          )    
   RAISERROR ('Not expecting Reader Table: %s & Writer Table: %s',16,1, @WasReaderTable, @WasWriterTable);   
  ELSE
    BEGIN               

      BEGIN TRY

        SET @ReaderViewAlterScript = REPLACE(REPLACE( OBJECT_DEFINITION(OBJECT_ID('dbo.vSourceDataReader'))
                                                    , @WasReaderTable
                                                    , @WasWriterTable)
                                             , 'CREATE VIEW '
                                             , 'ALTER VIEW ');
        SET @WriterViewAlterScript = REPLACE(REPLACE( OBJECT_DEFINITION(OBJECT_ID('dbo.vSourceDataWriter'))
                                                    , @WasWriterTable
                                                    , @WasReaderTable)
                                             , 'CREATE VIEW '
                                             , 'ALTER VIEW '); 

        SET @SQLStr = N'TRUNCATE TABLE [dbo].[' + @WasReaderTable + N'];' 
        EXEC sp_executesql @SQLStr;

        IF EXISTS (SELECT * FROM sys.indexes 
                   WHERE object_id = OBJECT_ID(@WasReaderTable)
                   AND name = @IndexName)
           BEGIN
             SELECT @AllowRowLocks = CASE WHEN allow_row_locks = 1 THEN 'ON' ELSE 'OFF' END 
                  , @AllowPageLocks = CASE WHEN allow_page_locks = 1 THEN 'ON' ELSE 'OFF' END 
             FROM sys.indexes 
             WHERE object_id = OBJECT_ID(@WasReaderTable)
             AND name = @IndexName
             
             SET @SQLStr = N'DROP INDEX [' + @IndexName + N'] ON [dbo].[' + @WasReaderTable + N'];';
             EXEC sp_executesql @SQLStr;
           END 

         IF @LoggingLevel > 1
           SET @TimerDt = SYSDATETIME(); 

           -- reader is empty and unused so instead of figuring out which one, just lock them both   
           BEGIN TRANSACTION;  

             SET @ResourceLocker = (SELECT TOP(1) 1
                                    FROM $(StagingDB).dbo.SourceDataA a WITH (XLOCK, TABLOCK) 
                                    UNION 
                                    SELECT TOP(1) 1
                                    FROM $(StagingDB).dbo.SourceDataB b WITH (XLOCK, TABLOCK));
             
             
             EXEC sp_executesql @ReaderViewAlterScript;
             EXEC sp_executesql @WriterViewAlterScript;

           COMMIT TRANSACTION;
         
         IF @LoggingLevel > 1
           SET @SwapViews_ms = DATEDIFF(ms, @TimerDT, SYSDATETIME()); 

         IF @LoggingLevel > 1
           SET @TimerDt = SYSDATETIME(); 
   
         SET @SQLStr = N'CREATE INDEX [' + @IndexName + N'] ' 
                     + N'ON [dbo].[' + @WasReaderTable + N'] ([Name], [LOBCheckSum], [GMTDateTime]) '
                     + N'WITH ( ALLOW_ROW_LOCKS = ' + @AllowRowLocks 
                     + '      , ALLOW_PAGE_LOCKS = ' + @AllowPageLocks + ');';
     
         EXEc sp_executesql @SQLStr;

         IF @LoggingLevel > 1
           BEGIN
             SET @IndexHeap_ms = DATEDIFF(ms, @TimerDT, SYSDATETIME()); 

             SELECT  DATEDIFF(ms, @StartDT, SYSDATETIME()) AS [ChangeView Elapsed ms]
                   , @SwapViews_ms AS [ms to swap views]  
                   , @SwapViews_ms/DATEDIFF(ms, @StartDT, SYSDATETIME()) AS [% elapsed] 
                   , @IndexHeap_ms AS [ms to index heap]
                   , @IndexHeap_ms/DATEDIFF(ms, @StartDT, SYSDATETIME()) AS [% elapsed];
           END 
      
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
          ROLLBACK TRANSACTION;

        RAISERROR ('Msg %d, Level %d, State %d, %s Line %d %s'
                  ,16 ,1 ,@Number, @Severity, @State, @Proc, @Line, @Message)
          
      END CATCH;   

             
    END;

END;

GO

IF OBJECT_ID (N'dbo.TargetUpsert', N'P') IS NOT NULL
    DROP PROCEDURE dbo.TargetUpsert;

GO

CREATE PROCEDURE dbo.TargetUpsert 
  ( @LoggingLevel TINYINT
  , @LoadMethod NVARCHAR(128) 
  , @CreateIndex_ms BIGINT OUTPUT
  , @MergeParentRows BIGINT OUTPUT
  , @MergeParent_ms BIGINT OUTPUT
  , @IndexMapper_ms BIGINT OUTPUT
  , @InsertChildRows BIGINT OUTPUT
  , @InsertChild_ms BIGINT OUTPUT)
AS
BEGIN

 DECLARE @RowCount INT
       , @TimerDT DATETIME2;

 -- needs index for large rowsets
 CREATE TABLE #MergeMapper 
   ( Id BIGINT PRIMARY KEY
   , Name VARCHAR(50)
   , LOBCheckSum INT
   , GMTDate DATE
   , GMTHour INT);

 IF NOT EXISTS (SELECT * FROM sys.indexes 
                WHERE name = 'ixn_StagedData__Name__LOBSize__GMTDateTime'
                AND object_id = OBJECT_ID('dbo.StagedData'))
   BEGIN
   
     IF @LoggingLevel > 1
       SET @TimerDT = SYSDATETIME(); 

     -- locking may be better managed in-line, more flexible
     -- the intention is to always force an HOBT lock
     CREATE INDEX [ixn_StagedData__Name__LOBSize__GMTDateTime]
     ON dbo.StagedData([Name], [LOBCheckSum], [GMTDateTime]);

     --WITH (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF);
 
     --ALTER TABLE dbo.StagedData
     --SET (LOCK_ESCALATION = AUTO);

     IF @LoggingLevel > 1
       SET @CreateIndex_ms = DATEDIFF(ms, @TimerDT, SYSDATETIME()); 
   END;

 IF @LoggingLevel > 1
   SET @TimerDT = SYSDATETIME(); 

 MERGE $(TARGETDB).dbo.tParent AS p
 USING ( SELECT [Name]
              , [LOBCheckSum]
              , CAST([GMTDateTime] AS DATE) AS [GMTDate]
              , DATEPART(hour, [GMTDateTime]) AS [GMTHour]
              , MIN(LOB) AS [LOB] -- since LOBCheckSum determines equality no need to look in the LOB, take the first one
              , COUNT(*) AS [Occurrences]  
         FROM dbo.StagedData
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
   SET @MergeParent_ms = DATEDIFF(ms, @TimerDT, SYSDATETIME());

 IF @LoggingLevel > 1
   SET @TimerDT = SYSDATETIME(); 
 
 -- could skip for small rowsets but not much advantage
 CREATE INDEX ixn_#MergeMapper
 ON #MergeMapper( [Name], [LOBCheckSum], [GMTDate], [GMTHour])
 
 IF @LoggingLevel > 1
   SET @IndexMapper_ms = DATEDIFF(ms, @TimerDT, SYSDATETIME());

 IF @LoggingLevel > 1
   SET @TimerDT = SYSDATETIME(); 

 INSERT $(TARGETDB).dbo.tChild 
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
 JOIN dbo.StagedData d
 ON p.[Name] = d.[Name]
 AND p.[LOBCheckSum] = d.[LOBCheckSum]
 AND p.[GMTDate] = CAST(d.GMTDateTime AS DATE)
 AND p.[GMTHour] = DATEPART(hour, d.GMTDateTime);
    
 SET @InsertChildRows = @@ROWCOUNT;

 IF @LoggingLevel > 1
   SET @InsertChild_ms = DATEDIFF(ms, @TimerDT, SYSDATETIME());

 -- leave the data in the staging table until next import 

END;

GO


IF OBJECT_ID (N'dbo.BulkInsertUpsert', N'P') IS NOT NULL
    DROP PROCEDURE dbo.BulkInsertUpsert;
GO

CREATE PROCEDURE dbo.BulkInsertUpsert
  ( @FullFilePath NVARCHAR(640)
  , @DataTypes NVARCHAR(20)
  , @LoggingLevel TINYINT
  , @LoadMethod NVARCHAR(128) 
  , @DropIndex_ms BIGINT OUTPUT
  , @BulkInsertRows BIGINT OUTPUT
  , @BulkInsert_ms BIGINT OUTPUT
  , @CreateIndex_ms BIGINT OUTPUT
  , @MergeParentRows BIGINT OUTPUT
  , @MergeParent_ms BIGINT OUTPUT
  , @IndexMapper_ms BIGINT OUTPUT
  , @InsertChildRows BIGINT OUTPUT
  , @InsertChild_ms BIGINT OUTPUT )
AS
BEGIN

  DECLARE @BatchSize INT
        , @SQLStr NVARCHAR(1000)
        , @TimerDT DATETIME2;

  -- the rows sent is irrelevant to the load, defined at data origination not here
  SET @BatchSize = (SELECT [RowsToPush] FROM dbo.tTransferParameters);
  CREATE TABLE #MergeMapper 
    ( [Id] BIGINT PRIMARY KEY
    , [Name] VARCHAR(50)
    , [LOBCheckSum] INT
    , [GMTDate] DATE
    , [GMTHour] INT);

   IF @LoggingLevel > 1
     SET @TimerDT = SYSDATETIME(); 

   -- get the table ready
   IF EXISTS (SELECT * FROM sys.indexes 
              WHERE name = 'ixn_StagedData__Name__LOBSize__GMTDateTime'
              AND object_id = OBJECT_ID('dbo.StagedData'))
    DROP INDEX [ixn_StagedData__Name__LOBSize__GMTDateTime] ON dbo.StagedData;
   TRUNCATE TABLE dbo.StagedData;

   IF @LoggingLevel > 1
     SET @DropIndex_ms = DATEDIFF(ms, @TimerDT, SYSDATETIME());

   IF @LoggingLevel > 1
     SET @TimerDT = SYSDATETIME(); 

   SET @SQLStr = N'BULK INSERT $(STAGINGDB).dbo.StagedData '
               + N'FROM ''' + @FullFilePath + N''' '
               + N'WITH ( BATCHSIZE = ' + CAST(@Batchsize AS NVARCHAR(10)) 
               + N'     , DATAFILETYPE = ''' + @DataTypes + N''' '
               + N'     , TABLOCK '
               + N'     , ROWS_PER_BATCH  = ' + CAST(@Batchsize AS NVARCHAR(10))
               + N'     , ORDER ([RowNumber]));'

   EXEC sp_executesql @SQLStr;             
          
   SET @BulkInsertRows = @@ROWCOUNT; 

   IF @LoggingLevel > 1
     SET @BulkInsert_ms =  DATEDIFF(ms, @TimerDT, SYSDATETIME()); 

 IF NOT EXISTS (SELECT * FROM sys.indexes 
                WHERE name = 'ixn_StagedData__Name__LOBSize__GMTDateTime'
                AND object_id = OBJECT_ID('dbo.StagedData'))
   BEGIN
   
     IF @LoggingLevel > 1
       SET @TimerDT = SYSDATETIME(); 

     CREATE INDEX [ixn_StagedData__Name__LOBSize__GMTDateTime]
     ON dbo.StagedData([Name], [LOBCheckSum], [GMTDateTime]);
 
     IF @LoggingLevel > 1
       SET @CreateIndex_ms = DATEDIFF(ms, @TimerDT, SYSDATETIME());
        
   END;

   IF @LoggingLevel > 1
     SET @TimerDT = SYSDATETIME(); 

   MERGE $(TARGETDB).dbo.tParent AS p
   USING ( SELECT [Name]
                , [LOBCheckSum]
                , CAST([GMTDateTime] AS DATE) AS [GMTDate]
                , DATEPART(hour, [GMTDateTime]) AS [GMTHour]
                , MIN(LOB) AS [LOB] -- since LOBCheckSum determines equality no need to look in the LOB
                , COUNT(*) AS [Occurrences]  
           FROM dbo.StagedData
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
     SET @MergeParent_ms = DATEDIFF(ms, @TimerDT, SYSDATETIME()); 

   IF @LoggingLevel > 1
     SET @TimerDT = SYSDATETIME(); 
   
   CREATE INDEX ixn_#MergeMapper
   ON #MergeMapper( [Name], [LOBCheckSum], [GMTDate], [GMTHour])
   
   IF @LoggingLevel > 1
     SET @IndexMapper_ms = DATEDIFF(ms, @TimerDT, SYSDATETIME()); 

   IF @LoggingLevel > 1
     SET @TimerDT = SYSDATETIME(); 

   INSERT $(TARGETDB).dbo.tChild 
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
   JOIN dbo.StagedData d
   ON p.[Name] = d.[Name]
   AND p.[LOBCheckSum] = d.[LOBCheckSum]
   AND p.[GMTDate] = CAST(d.GMTDateTime AS DATE)
   AND p.[GMTHour] = DATEPART(hour, d.GMTDateTime);
      
   SET @InsertChildRows = @@ROWCOUNT;

   -- leave the data in the staging table until next import 
   -- large received rowsets may need and index on the mapper      

   IF @LoggingLevel > 1
     SET @InsertChild_ms = DATEDIFF(ms, @TimerDT, SYSDATETIME());

END;

GO

