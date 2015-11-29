:SETVAR SQLSERVER "BILL764\ATER2"
---- STYLE is always "AB"                                
:SETVAR STAGINGDB "staging"                                                            
:SETVAR TARGETDB "target"                                                              
:SETVAR LOCKTYPE "ROW"                  -- ROW  PAG  TAB                               
:SETVAR ISOTYPE "VER"                   -- VER  LCK                                    
:SETVAR CONCURRENTCOUNT 1                                                              
:SETVAR SCRIPTPATH "C:\Users\bwunder\Documents\SQL Server Management Studio\Projects\data load methods\data load methods\" 

------------------------------------------------------------------------------------------
-- send any combination of singleton inserts, batches and sets of rows to one of a pair of 
-- identical staging tables through an insert view. 

-- Read from a different view on the same table to when upserting the staged data into the 
-- target hierarchy 

-- periodically ALTER the Views so that insert view is using an empty unindexed staging
-- table and the upsert view is attached to the table of new data and the upsert process  
-- started in background. 

-- Only subsecond pause in collection required during switch. 

-- target latency determined by switch interval

-- unlike other methods, external data is moved into the SQL Server asynchrounously from data 
-- being loading into the target hierearchy. Rows are always processed in the order received.
--------------------------------------------------------------------------------------------
SET NOCOUNT ON;


GO

use $(STAGINGDB);

GO

IF EXISTS (SELECT *
           FROM sys.databases
           WHERE database_id = DB_ID()
           AND is_read_committed_snapshot_on = 0
           AND '$(ISOTYPE)' = 'VER') 
 BEGIN

  ALTER DATABASE $(STAGINGDB)
  SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

  ALTER DATABASE $(STAGINGDB)
  SET READ_COMMITTED_SNAPSHOT ON;

  ALTER DATABASE $(STAGINGDB)
  SET MULTI_USER;

 END

IF EXISTS (SELECT * 
           FROM sys.databases
           WHERE name = '$(STAGINGDB)'
           AND is_read_committed_snapshot_on = 1
           AND '$(ISOTYPE)' = 'LCK') 
 BEGIN

  ALTER DATABASE $(STAGINGDB)
  SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

  ALTER DATABASE $(STAGINGDB)
  SET READ_COMMITTED_SNAPSHOT OFF;

  ALTER DATABASE $(STAGINGDB)
  SET MULTI_USER;

 END

-- we never want table locks IF they can be avoided
IF NOT EXISTS (SELECT * FROM $(STAGINGDB).sys.tables 
               WHERE name  = 'SourceDataA'
               AND schema_id = 1 
               AND lock_escalation = 1)
  ALTER TABLE $(STAGINGDB).dbo.SourceDataA
  SET (LOCK_ESCALATION = DISABLE);

IF NOT EXISTS (SELECT * FROM $(STAGINGDB).sys.tables 
               WHERE name  = 'SourceDataB' 
               AND schema_id = 1 
               AND lock_escalation = 1)
  ALTER TABLE $(STAGINGDB).dbo.SourceDataB
  SET (LOCK_ESCALATION = DISABLE);

-- we want either row or page locks, not both IF it can be avoided
IF ('$(LOCKTYPE)' = 'ROW')
  BEGIN
    IF EXISTS (SELECT * FROM $(STAGINGDB).sys.indexes 
               WHERE object_id IN (OBJECT_ID('dbo.SourceDataA'), OBJECT_ID('dbo.SourceDataB'))
               AND (allow_row_locks = 0 
                    OR allow_page_locks = 1) )
      BEGIN
        ALTER INDEX ALL
        ON $(STAGINGDB).dbo.SourceDataA
        SET (ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = OFF); 

        ALTER INDEX ALL
        ON $(STAGINGDB).dbo.SourceDataB
        SET (ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = OFF); 
      END

  END

IF ('$(LOCKTYPE)'  = 'PAG')
  BEGIN
    IF EXISTS (SELECT * FROM $(STAGINGDB).sys.indexes 
               WHERE object_id IN (OBJECT_ID('dbo.SourceDataA'), OBJECT_ID('dbo.SourceDataB'))
               AND (allow_row_locks = 1 
                    OR allow_page_locks = 0) )
      BEGIN
        ALTER INDEX ALL
        ON $(STAGINGDB).dbo.SourceDataA
        SET (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = ON); 

        ALTER INDEX ALL
        ON $(STAGINGDB).dbo.SourceDataB
        SET (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = ON);
      END

  END

GO

IF EXISTS (SELECT * 
           FROM sys.databases
           WHERE name = '$(TARGETDB)'
           AND is_read_committed_snapshot_on = 1
           AND '$(ISOTYPE)' = 'VER') 
 BEGIN

  EXEC sp_executesql N'USE $(TARGETDB);

                     ALTER DATABASE $(TARGETDB)
                     SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

                     ALTER DATABASE $(TARGETDB)
                     SET READ_COMMITTED_SNAPSHOT ON;

                     ALTER DATABASE $(TARGETDB)
                     SET MULTI_USER;'

 END

IF EXISTS (SELECT *
           FROM sys.databases
           WHERE name = '$(TARGETDB)'
           AND is_read_committed_snapshot_on = 0
           AND '$(ISOTYPE)' = 'LCK')
 BEGIN

  EXEC sp_executesql N'USE $(TARGETDB);

                       ALTER DATABASE $(TARGETDB)
                       SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

                       ALTER DATABASE $(TARGETDB)
                       SET READ_COMMITTED_SNAPSHOT OFF;

                       ALTER DATABASE $(TARGETDB)
                       SET MULTI_USER;'
 END
 
GO

--select * from sys.tables WHERE name in ('tParent','tChild')
--select * from sys.indexes WHERE object_id in (OBJECT_ID('dbo.tParent'),OBJECT_ID('dbo.tChild'))

-- we never want table locks IF they can be avoided
IF NOT EXISTS (SELECT * FROM $(TARGETDB).sys.tables 
               WHERE name  = 'tParent'
               AND schema_id = 1 
               AND lock_escalation = 1)
  ALTER TABLE $(TARGETDB).dbo.tParent
  SET (LOCK_ESCALATION = DISABLE);

IF NOT EXISTS (SELECT * FROM $(TARGETDB).sys.tables 
               WHERE name  = 'tChild' 
               AND schema_id = 1 
               AND lock_escalation = 1)
  ALTER TABLE $(TARGETDB).dbo.tChild
  SET (LOCK_ESCALATION = DISABLE);

-- we want either row or page locks, not both IF it can be avoided
IF ('$(LOCKTYPE)' = 'ROW')
  BEGIN
    IF EXISTS (SELECT * FROM $(TARGETDB).sys.indexes 
               WHERE object_id IN (OBJECT_ID('dbo.tParent'), OBJECT_ID('dbo.tChild'))
               AND (allow_row_locks = 0 
                    OR allow_page_locks = 1) )
      BEGIN
        ALTER INDEX ALL
        ON $(TARGETDB).dbo.tParent
        SET (ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = OFF); 

        ALTER INDEX ALL
        ON $(TARGETDB).dbo.tChild
        SET (ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = OFF); 
      END

  END

IF ('$(LOCKTYPE)'  = 'PAG')
  BEGIN
    IF EXISTS (SELECT * FROM $(TARGETDB).sys.indexes 
               WHERE object_id IN (OBJECT_ID('dbo.tParent'), OBJECT_ID('dbo.tChild'))
               AND (allow_row_locks = 1 
                    OR allow_page_locks = 0) )
      BEGIN
        ALTER INDEX ALL
        ON $(TARGETDB).dbo.tParent
        SET (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = ON); 

        ALTER INDEX ALL
        ON $(TARGETDB).dbo.tChild
        SET (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = ON);
      END

  END

GO

IF OBJECT_ID('dbo.ABLoad', 'P') IS NOT NULL
  DROP PROCEDURE dbo.ABLoad

GO

CREATE PROCEDURE dbo.ABLoad
  ( @RowsToPush INT )
AS
 BEGIN
  DECLARE @MessageCount INT
        , @MessageQuantity INT
        , @LoggingLevel TINYINT
        , @LoadMethod NVARCHAR(128)
        , @RowCount INT
        , @LoadedRows BIGINT
        , @Load_ms BIGINT
        , @SwapViews_ms BIGINT
        , @MergeParentRows BIGINT
        , @MergeParent_ms BIGINT
        , @IndexMapper_ms BIGINT
        , @InsertChildRows BIGINT
        , @InsertChild_ms BIGINT
        , @StartDT DATETIME2
        , @TimerDT DATETIME2;

  SET @LoadMethod = OBJECT_NAME(@@PROCID);
  SET @MessageQuantity = (SELECT MessageQuantity FROM dbo.tTransferParameters);    
  SET @LoggingLevel = (SELECT LoggingLevel FROM dbo.tTransferParameters);

  SET @LoadedRows = 0;
  SET @Load_ms = 0;
  SET @MessageCount = 0;
  SET @StartDT = SYSDATETIME();

  BEGIN TRY

   WHILE @MessageCount < @MessageQuantity
    
    BEGIN

     -- is implicit anyway - a single atomic insert
     -- explicit txn gives better control of rollback/recovery
     BEGIN TRANSACTION;

      SET @TimerDT = SYSDATETIME();

       INSERT dbo.vSourceDataWriter 
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
        FROM dbo.fGetTableVariable(ISNULL(NULLIF(@MessageCount * @RowsToPush,0), 1) , @RowsToPush); 

        SET @LoadedRows += @@ROWCOUNT; 

        IF @LoggingLevel > 0
          SET @Load_ms += DATEDIFF(ms, @TimerDT, SYSDATETIME());

        SET @MessageCount += 1; 

      COMMIT TRANSACTION;
           
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

    RAISERROR ('Msg %d, Level %d, State %d, %s Line %d  %s'
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
    , LoggingLevel )
  SELECT OBJECT_NAME(@@PROCID)
       + CASE WHEN d.is_read_committed_snapshot_on = 0 
              THEN 'LCK' ELSE 'VER' END
       + CASE WHEN i.allow_row_locks = 1
              THEN 'ROW' ELSE 'PAG' END
       + c.Concurrency                     
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
  FROM dbo.tTransferParameters p
  CROSS JOIN sys.databases d
  CROSS JOIN $(TARGETDB).sys.indexes i
  CROSS APPLY (SELECT ISNULL(MAX(REPLACE(name,'ABLoad','')), '-1') AS Concurrency
               FROM msdb.dbo.sysjobs
               WHERE name like 'ABLoad%'
               AND enabled = 1) AS c 
  WHERE d.name = DB_NAME()
  AND i.object_id = OBJECT_ID('$(TARGETDB).dbo.tParent')
  AND i.Index_id = 1;

 END;

GO

USE msdb;

GO

IF EXISTS (SELECT * FROM dbo.sysjobs
           WHERE name = N'ABMergeJob')
  EXEC dbo.sp_delete_job @job_name = N'ABMergeJob';           

IF EXISTS (SELECT * FROM dbo.sysschedules
           WHERE name = N'ABMergeSchedule')
  EXEC dbo.sp_delete_schedule @schedule_name = N'ABMergeSchedule';            

EXEC dbo.sp_add_job @job_name = N'ABMergeJob' 
                  , @enabled = 0
                  , @description = N'take the currently active receiver out of services and move contents into the target.'
                  , @owner_login_name = N'sa';

EXEC dbo.sp_add_jobserver @job_name = N'ABMergeJob';

EXEC sp_add_jobstep @job_name = N'ABMergeJob'
                  , @step_name = N'change the staging reader & writer containers'
                  , @subsystem = N'TSQL'
                  , @command = N'EXEC $(STAGINGDB).dbo.ABChangeView'
                  , @retry_attempts = 1
                  , @retry_interval = 1
                  , @on_success_action = 3;

EXEC sp_add_jobstep @job_name = N'ABMergeJob'
                  , @step_name = N'add data to target hierarchy'
                  , @subsystem = N'TSQL'
                  , @command = N'EXEC $(STAGINGDB).dbo.ABViewMerge'
                  , @retry_attempts = 2
                  , @retry_interval = 1;
    
EXEC sp_add_schedule @schedule_name = N'ABMergeSchedule'
                   , @enabled = 1  
                   , @freq_type = 4
                   , @freq_interval = 1
                   , @freq_subday_type = 0x4
                   , @freq_subday_interval = 1;

EXEC sp_attach_schedule @job_name = N'ABMergeJob'
                      , @schedule_name = N'ABMergeSchedule';

GO

IF EXISTS (SELECT * FROM dbo.sysjobs
           WHERE name = N'ABUpsertJob')
  EXEC dbo.sp_update_job @job_name = N'ABUpsertJob'
                       , @enabled = 0;              

IF EXISTS (SELECT * FROM dbo.sysjobs
           WHERE name = N'ABUpsertJob')
  EXEC dbo.sp_delete_job @job_name = N'ABUpsertJob';           

IF EXISTS (SELECT * FROM dbo.sysschedules
           WHERE name = N'ABUpsertSchedule')
  EXEC dbo.sp_delete_schedule @schedule_name = N'ABUpsertSchedule';            

EXEC dbo.sp_add_job @job_name = N'ABUpsertJob' 
                  , @enabled = 0
                  , @description = N'take the currently active receiver out of services and move contents into the target.'
                  , @owner_login_name = N'sa';

EXEC dbo.sp_add_jobserver @job_name = N'ABUpsertJob';

EXEC sp_add_jobstep @job_name = N'ABUpsertJob'
                  , @step_name = N'change the staging reader & writer containers'
                  , @subsystem = N'TSQL'
                  , @command = N'EXEC $(STAGINGDB).dbo.ABChangeView'
                  , @retry_attempts = 2
                  , @retry_interval = 1
                  , @on_success_action = 3;

EXEC sp_add_jobstep @job_name = N'ABUpsertJob'
                  , @step_name = N'add data to target hierarchy'
                  , @subsystem = N'TSQL'
                  , @command = N'EXEC $(STAGINGDB).dbo.ABViewUpsert'
                  , @retry_attempts = 2
                  , @retry_interval = 1;
    
EXEC sp_add_schedule @schedule_name = N'ABUpsertSchedule'
                   , @enabled = 1  
                   , @freq_type = 4
                   , @freq_interval = 1
                   , @freq_subday_type = 0x4
                   , @freq_subday_interval = 1;

EXEC sp_attach_schedule @job_name = N'ABUpsertJob'
                      , @schedule_name = N'ABUpsertSchedule';

GO

-- (re)create 10 disabled jobs to simulate 10 concurrent processes sending data
-- with jobs we can easily implement retry and the query governor
-- ditto for service broker and smarter retry possible but more work that does not have anything to with current test objective  
!!FOR /L %i IN (0,1,9) DO Start SQLCMD -S$(SQLSERVER) -E -dmsdb -e -i "$(SCRIPTPATH)ABCreateJob.sql" -v STAGINGDB=$(STAGINGDB) JOBNUMBER=%i
