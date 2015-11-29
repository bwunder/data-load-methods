-----------------------------------------
-- contents                         line 
-- informational queries            10
-- debug aid                        40
-- enable queues and/or activation  75
-----------------------------------------

-- DMVs
--select * from sys.dm_broker_forwarded_messages
select object_name(queue_id), * from sys.dm_broker_queue_monitors WHERE database_id = DB_ID('initiator');
select object_name(queue_id), * from sys.dm_broker_queue_monitors WHERE database_id = DB_ID('target');
select * from target.sys.dm_broker_activated_tasks
select * from initiator.sys.dm_broker_activated_tasks
select * from sys.dm_broker_connections

--Service Broker objects
-- SQLRowAsyncMerger or SQLTVPAsyncMerge starts a conversation
select * from initiator.dbo.initiator_queue -- sender writes message here
select * from sys.transmission_queue  -- system object - sql server moves message here when it pulls it off the initiator
select * from target.dbo.target_queue WITH(NOLOCK) -- then delivers it here when it can
--the activation proc, pTargetActivationProcedure, running on background thread(s), pulls from the target_queue

select name
     , activation_procedure
     , [is_activation_enabled]
     , [is_receive_enabled]
     , [is_enqueue_enabled] 
from target.sys.service_queues  
WHERE name  = 'target_queue'
UNION ALL
select name
     , activation_procedure
     , [is_activation_enabled]
     , [is_receive_enabled]
     , [is_enqueue_enabled] 
from initiator.sys.service_queues i
WHERE name  = 'initiator_queue';

-- initiator side error handlings
select * from intiator.dbo.initiator_processing_errors;
select * from initiator.dbo.unsent_messages;

-- target side error handling
select * from target.dbo.tTargetProcessingErrors;
select * from target.dbo.tTargetMessageCounter;

select service_contract_name, message_type, DATALENGTH(message_body) as MessageSize 
from target.dbo.target_queue

exec target.dbo.pGetQueueSizes


/* debugging the activation procedure
disable ACTIVATION 
step into the procedure
if you disable the queue you cannot put anything in it

  use target;

not this! it will re-enable or disable everything about the queue state
  --ALTER QUEUE target_queue
  --WITH STATUS = ON or OFF

this will allow messages to be sent to the target queue 
only the activation procedure will not run automatically 
  ALTER QUEUE target_queue
  WITH ACTIVATION (Status = OFF)

run this initator script to put some data into the broker conversation
but skip the check for non-active queue state above
then select the following and hit F11 twice to step into the proc

   [target].[dbo].[pTargetActivationProcedure]

in case a txn is open try

   WHILE @@TRANCOUNT > 0
     ROLLBACK;

then turn the queue back on

  ALTER QUEUE target_queue
  WITH ACTIVATION (Status = ON);


--select cast(message_body as NVARCHAR(MAX)), * from target..target_queue

   use master;

*/


/*
-- to enable queue (so that [is_receive_enabled] = 1, [is_enqueue_enabled] = 1)
ALTER QUEUE initiator_queue
WITH STATUS = ON
-or-
ALTER QUEUE target_queue
WITH STATUS = Off
-- to enable activation (so that [is_activation_enabled] = 1)
ALTER QUEUE initiator_queue
WITH ACTIVATION (STATUS = ON);
-or-
ALTER QUEUE target_queue
WITH ACTIVATION (STATUS = ON);
-- to debug the activation procedure, drop it activation and step into procedure
ALTER QUEUE initiator_queue
WITH ACTIVATION (STATUS = ON);
-or-
ALTER QUEUE target_queue
WITH ACTIVATION (STATUS = ON);
-- to start debug, using SSMS highlight following line hit F11 twice
[dbo].[pTargetActivationProcedure]
*/

/* clearing the target queue
SELECT * FROM sys.conversation_endpoints

SELECT 'END CONVERSATION ''' + CAST(conversation_handle AS NVARCHAR(MAX)) + ''' WITH CLEANUP' 
FROM sys.conversation_endpoints
*/

/*
ALTER QUEUE target_queue
WITH ACTIVATION(MAX_QUEUE_READERS = 4)


*/