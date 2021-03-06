/*
KILL 53
*/
:SETVAR TARGETDB "target"         --                                                       
:SETVAR INITIATORDB "initiator"   --                                                       
:SETVAR STAGINGDB "staging"       --                                                       
:SETVAR DROPTARGET 1              -- 1=yes ANYTHING ELSE = NO                              
:SETVAR DROPINITIATOR 1           -- 1=yes ANYTHING ELSE = NO (MAY AFFECT TEST CONTINUITY) 
:SETVAR DROPSTAGING 1             -- 1=yes ANYTHING ELSE = NO                              

USE master;
GO

IF EXISTS (SELECT * FROM sys.databases WHERE name = '$(STAGINGDB)' AND $(DROPSTAGING) = 1)
    IF NOT EXISTS (SELECT * FROM sys.sysprocesses 
                   WHERE DBID = DB_ID('$(STAGINGDB)')
                   AND spid > 49)
 
        BEGIN   

            IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'LoadFromStagedBCP')
                EXEC msdb.dbo.sp_delete_job @job_name=N'LoadFromStagedBCP', @delete_unused_schedule=1

            IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'LoadFromABTable')
                EXEC msdb.dbo.sp_delete_job @job_name=N'LoadFromABTable', @delete_unused_schedule=1

            IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'source1')
                EXEC msdb.dbo.sp_delete_job @job_name=N'source1', @delete_unused_schedule=1

            IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'source2')
                EXEC msdb.dbo.sp_delete_job @job_name=N'source2', @delete_unused_schedule=1

            IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'source3')
                EXEC msdb.dbo.sp_delete_job @job_name=N'source3', @delete_unused_schedule=1

            IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'source4')
                EXEC msdb.dbo.sp_delete_job @job_name=N'source4', @delete_unused_schedule=1

            IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'source5')
                EXEC msdb.dbo.sp_delete_job @job_name=N'source5', @delete_unused_schedule=1

            IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'source6')
                EXEC msdb.dbo.sp_delete_job @job_name=N'source6', @delete_unused_schedule=1

            IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'source7')
                EXEC msdb.dbo.sp_delete_job @job_name=N'source7', @delete_unused_schedule=1

            IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'source8')
                EXEC msdb.dbo.sp_delete_job @job_name=N'source8', @delete_unused_schedule=1

            IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'source9')
                EXEC msdb.dbo.sp_delete_job @job_name=N'source9', @delete_unused_schedule=1

            IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'source10')
                EXEC msdb.dbo.sp_delete_job @job_name=N'source10', @delete_unused_schedule=1

            DROP DATABASE $(STAGINGDB);

        END
    ELSE
        BEGIN
            SELECT spid as [session_id to disconnect]
                 , status
                 , last_batch
                 , program_name
                 , hostname
                 , loginame
                 , cmd 
            FROM sys.sysprocesses 
            WHERE DBID = DB_ID('$(STAGINGDB)') 
            AND SPID > 49;
            IF @@ROWCOUNT = 0
              RAISERROR ('End all open transactions and re-run this script', 16,1);
            ELSE
              RAISERROR ('Disconnect the listed processes from the %s database then re-run this script!',16,1,'$(STAGINGDB)');    
        END;
GO 
IF EXISTS (SELECT * FROM sys.databases WHERE name = '$(TARGETDB)' AND $(DROPTARGET) = 1)
    IF NOT EXISTS (SELECT * FROM sys.sysprocesses 
                   WHERE DBID = DB_ID('$(TARGETDB)')
                   AND spid > 49)
        BEGIN

             -- rollback in-flight conversations - else cannot drop because "database is in use"
            ALTER DATABASE $(TARGETDB) SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            
            -- backup current test results to initiator only if initiator is persisted
            IF DB_ID('$(INITIATORDB)') IS NOT NULL AND $(DROPINITIATOR) = 0
            AND EXISTS (SELECT * FROM $(TARGETDB).dbo.tActivityHistory)
              BEGIN
                print 'SELECT * INTO $(INITIATORDB).dbo.tActivityHistory_' + CONVERT(VARCHAR(8),SYSDATETIME(),112) + ' FROM $(TARGETDB).dbo.tActivityHistory;';
--                EXEC('SELECT * INTO $(INITIATORDB).dbo.tActivityHistory_' + CONVERT(VARCHAR(8),SYSDATETIME(),112) + ' FROM $(TARGETDB).dbo.tActivityHistory;');
--                EXEC('SELECT * INTO $(INITIATORDB).dbo.tReceiveHistory_' + CONVERT(VARCHAR(8),SYSDATETIME(),112) + ' FROM $(TARGETDB).dbo.tReceiveHistory;');
              END;
              
            DROP DATABASE $(TARGETDB);
        END    
    ELSE
        BEGIN
            SELECT spid as [session_id to disconnect]
                 , status
                 , last_batch
                 , program_name
                 , hostname
                 , loginame
                 , cmd 
            FROM sys.sysprocesses 
            WHERE DBID = DB_ID('$(TARGETDB)') 
            AND SPID > 49;
            
            IF @@ROWCOUNT = 0
             RAISERROR ('End all open transactions and re-run this script', 16,1)
            ELSE
             RAISERROR ('Disconnect the listed processes from the %s database then re-run this script!',16,1,'$(TARGETDB)') 
        END
GO

IF EXISTS (SELECT * FROM sys.databases WHERE name = '$(INITIATORDB)' AND $(DROPINITIATOR) = 1)
    IF NOT EXISTS (SELECT * FROM sys.sysprocesses 
                   WHERE DBID = DB_ID('$(INITIATORDB)'))
        BEGIN

             -- this seems to get rid of any conversation gone wild. 
             -- ALTER DATABASE $(INITIATORDB) SET SINGLE_USER WITH ROLLBACK IMMEDIATE

            DROP DATABASE $(INITIATORDB);
             
        END    
    ELSE
        BEGIN
            SELECT spid as [session_id to disconnect]
                 , status
                 , last_batch
                 , program_name
                 , hostname
                 , loginame
                 , cmd 
            FROM sys.sysprocesses 
            WHERE DBID = DB_ID('$(INITIATORDB)') ;
            
            IF @@ROWCOUNT = 0
             RAISERROR ('End all open conversations and re-run this script', 16,1)
            ELSE
             RAISERROR ('Disconnect the listed processes from the %s database then re-run this script!',16,1,'$(INITIATORDB)')    
        END
GO

IF EXISTS (SELECT * FROM sys.endpoints WHERE name = 'ServiceBrokerEndpoint')
  DROP ENDPOINT ServiceBrokerEndpoint 

GO 

-- drop the broker login and the msdb inbound routes 


GO
USE master;

