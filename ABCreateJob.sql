--:SETVAR STAGINGDB "staging"
--:SETVAR JOBNUMBER 1
USE msdb;

 BEGIN

  IF EXISTS (SELECT * FROM dbo.sysjobs
             WHERE name = N'ABLoad$(JOBNUMBER)')
    BEGIN
      EXEC dbo.sp_delete_job @job_name = N'ABLoad$(JOBNUMBER)';              
    END

  IF EXISTS (SELECT * FROM dbo.sysschedules
             WHERE name = N'ABLoadSchedule$(JOBNUMBER)')
    EXEC dbo.sp_delete_schedule @schedule_name = N'ABLoadSchedule$(JOBNUMBER)';            

  EXEC dbo.sp_add_job @job_name = N'ABLoad$(JOBNUMBER)' 
                    , @enabled = 0
                    , @description = N'one concurrency stream'
                    , @owner_login_name = N'sa';

  EXEC dbo.sp_add_jobserver @job_name = N'ABLoad$(JOBNUMBER)';

  
  -- keep 'em in order and take the max jobnumber in the time range of the report
  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'1000.1.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 1000;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'1.1.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 1;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'1.2.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 1;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'5.1.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 5;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'5.2.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 5;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'10.1.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 10;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'10.2.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 10;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'50.1.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 50;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'50.2.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 50;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'100.1.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 100;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'100.2.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 100;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'500.1.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 500;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'500.2.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 500;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'1000.2.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 1000;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'5000.1.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 5000;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'5000.3.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 5000;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'1000.3.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 1000;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'1000.4.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 1000;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'500.3.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 500;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'500.4.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 500;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'100.3.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 100;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'100.4.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 100;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'50.3.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 50;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'50.4.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 50;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'10.3.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 10;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'10.4.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 10;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'5.3.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 5;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'5.4.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 5;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'1.3.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 1;'
                    , @retry_attempts = 2
                    , @retry_interval = 0 
                    , @on_fail_action = 3
                    , @on_success_action = 3;

  EXEC sp_add_jobstep @job_name = N'ABLoad$(JOBNUMBER)'
                    , @step_name = N'1.4.$(JOBNUMBER)'
                    , @subsystem = N'TSQL'
                    , @database_name = N'$(STAGINGDB)'
                    , @command = N'EXEC dbo.ABLoad @RowsToPush = 1'
                    , @retry_attempts = 2
                    , @retry_interval = 0
                    -- take the defaults on the last load in the job 
                    , @on_fail_action = 2
                    , @on_success_action = 1;

  EXEC sp_add_schedule @schedule_name = N'ABLoadSchedule$(JOBNUMBER)'
                     , @enabled = 1  
                     , @freq_type = 4
                     , @freq_interval = 1
                     , @freq_subday_type = 0x4
                     , @freq_subday_interval = 1
                     , @active_start_time = $(JOBNUMBER); -- stagger by a minute

  EXEC sp_attach_schedule @job_name = N'ABLoad$(JOBNUMBER)'
                        , @schedule_name = N'ABLoadSchedule$(JOBNUMBER)';
 END

GO

