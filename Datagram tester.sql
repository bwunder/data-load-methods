:SETVAR ROWSREQUESTED 100
--template
---------------------------------------------------------------------------
-- TVP to VARBINARY(MAX)
---------------------------------------------------------------------------
use target;

DECLARE @TimerDT DATETIME2;

-- reset timer
SET @TimerDT = SYSDATETIME();

DECLARE @FromTableVariable TVPTableType;
DECLARE @ToTableVariable TVPTableType;

/*
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
*/
DECLARE @RowCount BIGINT
      , @bigbin VARBINARY(MAX);

INSERT @FromTableVariable
  ( [RowNumber]
  , [Host]
  , [GMTDateTime]
  , [Name]
  , [Value]
  , [FloatingPoint]
  , [Boolean]
  , [Bytes]
  , [LOB]
  , [LOBCheckSum] )
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
FROM target.dbo.fGetTableVariable(1,$(ROWSREQUESTED)); 

-- prefix the data package with the rowcount for downstream validation. 
SET @RowCount = @@ROWCOUNT;

--select * from @FromTableVariable

---------------------------------------------------------------------------
-- TVP to VARBINARY(MAX)
---------------------------------------------------------------------------
/*
SELECT CAST(RowNumber AS BINARY(4)) as RowNumber                                                             
     , CAST(RowNumber AS VARBINARY(MAX)) as RowNumber                                                             
     , CAST(CAST(DATALENGTH(CAST(Host AS VARBINARY(20))) AS TINYINT) AS VARBINARY(MAX))             
     , CAST(Host AS VARBINARY(20)) as host
     --, CAST(CAST(DATALENGTH(CAST(GMTDateTime AS VARBINARY(MAX))) AS TINYINT) AS VARBINARY(MAX))   
     , CAST(GMTDateTime AS BINARY(8)) as GMTDateTime
     , CAST(CAST(DATALENGTH(CAST(Name AS VARBINARY(MAX))) AS TINYINT) AS VARBINARY(MAX))             
     , CAST(Name AS VARBINARY(40))                                                                  
     , CAST(Value AS BINARY(4))                                                                 
     , CAST(STR(FloatingPoint, 20, 16) AS VARBINARY(MAX))  
     , CAST(Boolean AS BINARY(1))
     , CAST(CAST(DATALENGTH(CAST(Bytes AS VARBINARY(MAX))) AS TINYINT) AS VARBINARY(MAX))            
     , Bytes                                                                 
     , CAST(CAST(DATALENGTH(CAST(LOB AS VARBINARY(MAX))) AS INT) AS VARBINARY(MAX))                  
     , CAST(LOB AS VARBINARY(MAX))                                                                   
     , CAST(CAST(DATALENGTH(LOB) AS INT) AS BINARY(4))
FROM @TableVariable;
*/

-- this is the meat of the fBinaryMarshal function
                 
SET @bigbin = CAST(@RowCount AS VARBINARY(MAX));
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
      FROM @FromTableVariable) derived

--SELECT @@ROWCOUNT AS [rows], @bigbin AS [as binary];

---------------------------------------------------------------------------
-- TVP to VARBINARY(MAX)
---------------------------------------------------------------------------



DECLARE @RowsInBlob BIGINT
      , @binrow VARBINARY(MAX)
      , @binrowlength INT
      , @blob VARBINARY(MAX)
      , @HostLength TINYINT
      , @NameLength TINYINT
      , @BytesLength TINYINT 
      , @LOBLength INT;

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

/*
select @HostLength AS [@HostLength]
     , @GMTDateTimeLength AS [@GMTDateTimeLength] 
     , @NameLength AS [@NameLength]
     , @BytesLength AS [@BytesLength]
     , @LOBLength AS [@LOBLength]; 
*/
    INSERT INTO @ToTableVariable      -- bytes
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

SELECT $(ROWSREQUESTED) AS [Rows Requested]
     , @RowsInBlob AS [Rows Sent]
     , COUNT(*) AS [Rows Received]
     , DATEDIFF(ms, @TimerDT, SYSDATETIME()) AS [Elapsed ms]
     , DATEDIFF(ms, @TimerDT, SYSDATETIME())/@RowCount AS [ms/row]
FROM @ToTableVariable; 

SELECT * 
FROM @FromTableVariable f
FULL OUTER JOIN @ToTableVariable t
ON f.RowNumber = t.RowNumber
WHERE f.Host <> t.Host
OR f.Bytes <> t.Bytes
OR f.GMTDateTime <> t.GMTDateTime
OR f.LOB <> t.LOB
OR f.LOBCheckSum = t.LOBCheckSum;

use master;