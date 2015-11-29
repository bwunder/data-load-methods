
-- the dbo.tTransferParameters table columns are described below

-- the other patameters are the values used during database creation
-- compression, isolation and indexing could have been changed since that time
-- Sync processes only care about MessageQuantity, RowsToPush and LoggingLevel
UPDATE target.dbo.tTransferParameters                                        -- domain     --(default) 
SET MessageQuantity             = 1           --MessageQuantity              -- 1-10000    --(3)
  , RowsToPush                  = 1000        --RowsToPush                   -- 1-10000    --(100)
  , ReceiveMsgWaitTime          = '00:00:00'  --ReceiveMsgWaitTime           -- hh:mm:ss   --(00:00:00)
  , NumberInitiatorTransactions = 1           --NumberInitiatorTransactions  --            --(1)
  , SendTxnWaitTime             = '00:00:00'  --SendTxnWaitTime              -- hh:mm:ss   --(00:00:00)
  , NumberDialogs               = 1           --NumberDialogs                --            --(1)
  , DialogRecycleMaxMessages    = 1           --DialogRecycleMaxMessages     --            --(1)
  , MaxQueueReaders             = 1           --MaxQueueReaders              --            --(1)
  , MaxMessagesPerReceive       = -1          --MaxMessagesPerReceive        --            --(-1)
                                              --!logging level 2 will impare performance
  , LoggingLevel                = 1           --LoggingLevel -- 0=none, 1=log, 2=+detail   --(1) 
--  ,	DateColumnIndexType  -- these columns journal the SQLCMD values used to create the test platform           
--  ,	IdColumnIndexType
--  , FillFactorPct
--  ,	DataCompression
--  ,	RowVersioning
FROM target.dbo.tTransferParameters;
-- validate specified parameters.
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

-- row based transfers always pregenerate MessageQuantity*RowsToPush source rows
-- then process them one at a time
SELECT MessageQuantity * RowsToPush	AS [RowsRequested]
     , *
FROM target.dbo.tTransferParameters;

----------------------------------------------------
-- The data push parameters.
--
-- Application parameters:
--
-- MessageQuantity*: number of messages to be sent. 
--
-- RowsToPush*: maximum number of rows in each message/batch or set. In general, the only part of the user 
--                  coded test system that cares about the actual number of rows that must be availalble
--                  or involved or about to be processed is the fetch. We measure the fetch time so
--                  that it can be factored out of test results. Data is created for the test in a 
--                  easily repeatable manner and is of only secondary concern to the question of how to 
--                  load rows.
--                  Here, a fetch to the ringbuffer DMV. The data provides a natural Parent/Child hierarchy.
--                  and could as easily be used to scale well beyond the tested transfer size fo 100,000 rows.  
--                  is somewhat contrived and used more becasue it almost has has 2000-3000 rows 
--                  avaialble to feed the test cycles and because it provides a variety of data types that 
--                  can be pushed through the transfer processing.                    
--                  
--                  Beyond making the distinction between row-at-a-time processing and set based 
--                  processing, the RowsToPush value is not considerered directly during load
--                  It is nonetheless important that the expected and actual RowsToPush be     
--                  consistently set across each test cycle, recored in the log as artifacts of
--                  each transfer, and appropriately considered during comparative analysis.
--
--                  WHile it is not a technique used here, it would be reasonable to transpose this
--                  into a batch counter requirement on the recieve, Something that would only operate
--                  at granularity less than the RECEIVE - subsets (any set is a subset of itself)
--                  the received rows. (http://rusanu.com/2006/10/16/writing-service-broker-procedures/)
--                
-- *RowsRequested = MessageQuantity * RowsToPush 
--        this is the value used to fetch a predetermined result set before the actual 
--        load test begins for row level transfers. For very large RowsRequested or RowsToPush values 
--        it may be necessary to reconsider that small transfer set (< 10, rows per transfer) optimzation.    
--  
-- ReceiveMsgWaitTime: pause added to the activiation proc after each message. When set to '00:00:00'
--                     no delay is introduced into broker processing. This provides a best case isolation
--                     for event timestamp base performance studies. Perhaps this is also useful in 
--                     load-balancing scenarios.
--
-- Internal parameters:
-- NumberInitiatorTransactions: number of initiator transactions used.
--    Notes: 1. Fewer is more efficient since each transaction entails an overhead.
--           2. Messages are actually sent when transaction commits, so sending a large
--              number of messages in a transaction can result in increased latency.
-- SendMsgWaitTime: pause added after each initiator transactions.
--    Notes: 1. A transaction can be thought of as a burst of MessageQuantity / 
--              NumberInitiatorTransactions messages. This delay specifies a time
--              to wait before the next transaction is run.
--           2. This parameter can be used to simulate message traffic distributed
--              over time.
--  NumberDialogs: number of dialogs used to send messages.
--    Notes: 1. Message ordering only guaranteed with a dialog.
--           2. Multiple dialogs allows concurrent processing on target.
--           3. Dialog creation is expensive; dialog reuse is employed here.
-- DialogRecycleMaxMessages: maximum number messages sent on a dialog before
--    recycling the dialog. Recycling is defined as ending the old dialog and
--    beginning a new one. A value of -1 indicates no recycling.
--    Notes: 1. Larger is more efficient since is minimizes the overhead of
--              creating dialogs.
--           2. Larger can complicate dialog error processing.
-- MaxQueueReaders: number of activated target procedures to receive messages.
--    Notes: 1. A target proc locks all messages in a dialog when it receives first message
--              for a dialog, blocking other procs from processing these messages.
--           2. Thus more dialogs yields increased concurrent processing. However, unless
--              dialog recycling is used, this should be set to NumberDialogs, which
--              can utilize a target proc for each dialog.
-- MaxMessagesPerReceive: maximum number of messages per target receive call.
--    Notes: 1. Larger is more efficient, but can complicate transaction error processing.
--           2. The maximum value can be set to MessageQuantity / NumberDialogs.
--
-- LoggingLevel: Amount of detail to write to ActivityLog duing processing. Any log writes
--                that do not affect the timing metrics are not affected by this value. Removing            
--                logging can provide a better representative time to comlete a row transfer
-- DateColumnIndexType 
-- & IdColumnIndexType: Common setup SQLCMD variable -- defines a CLUSTERED index on either the
--                  the Id or GMTDateTime or niether. These two parameters must be applied correctly.
--                  Only one can be specified CLUSTERED or the script will fail. Both can be NONCLUSTERED          
-- 
-- DataCompression: Common setup SQLCMD variable -- defines the compression set on all indexes 
--                   of the the tParent & tChild tables
-- RowVersioning: Common setup SQLCMD variable -- Indicated whether READ_COMMITTED_SNAPSHOT was enabled
--                 at the time the target db was created 
--
-- General note: for simplicity, @MessageQuantity should be evenly divisible
-- by @NumberInitiatorTransactions x @NumberDialogs, since this allows a
-- constant number of messages to be sent per dialog per transaction. "Remainder"
-- messages will not be sent to the target.
--
-- Example:
--
-- I want to send 100000 messages in sets of 10000 with a delay of 10 seconds between
-- each set. This calls for 10 transactions. Each message is 100 bytes and the target
-- message processing time is 10 ms. The messages are independent of each other, so use
-- 5 dialogs and target procedures to get some concurrent processing on the target. Allow
-- each target proc to receive 2000 messages at a time. Do not recycle dialogs.
--
-- INSERT INTO tTransferParameters
--       VALUES
--       (
--       100000,
--       10000,
--       '00:00:00:010',
--       10,
--       '00:00:10:000',
--       5,
--       -1,
--       5,
--       2000,
--       0
--       );
------------------------------------------------------------------------------------------