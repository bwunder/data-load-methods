-- target SQL instance & database 
:SETVAR SQLSERVER "BILL764\ATER2"                                                                                          
:SETVAR DB "staging"                   -- db context of this script                                                        
:SETVAR TARGETDB "target"                                                                                                  
:SETVAR STAGINGDB "staging"                                                                                                
:SETVAR STYLE "ROW"                     --  Row    TVP  NOT FOR AB!!!                                                      
:SETVAR METHOD "Upsert"                -- Merge  Upsert                                                                    
:SETVAR SCRIPTPATH "C:\Users\bwunder\Documents\SQL Server Management Studio\Projects\data load methods\data load methods\" 
:SETVAR TESTSCRIPT "SQLTestCycle.sql"  -- SQLTestCycle.sql  SSISTestCycle.sql                                              
-- do not automate interleave isolation changes, let one finish then start new
:SETVAR ISOTYPE "VER"                  -- VER - Row Versioning   LCK - Locking                                             

SET NOCOUNT ON;
GO

TRUNCATE TABLE $(TARGETDB).dbo.tChild;
DELETE $(TARGETDB).dbo.tParent;
GO
EXEC $(TARGETDB).dbo.pGetIndexDetails 'SAMPLED';

IF OBJECT_ID('tempdb.dbo.#CycleTimer','U') IS NOT NULL
  DROP TABLE #CycleTimer;

SELECT SYSDATETIME() AS StartTime INTO #CycleTimer;

GO
-- starts with row locks, switches to page locks mid sript
:SETVAR LOCKTYPE "ROW"    -- ROW - use row locks   PAG - use page locks         
GO
-- build the stored procedure
!!SQLCMD -S $(SQLSERVER) -E -d $(DB) -i "$(SCRIPTPATH)$(STYLE)GenerateProc.sql" -e -r1 -v METHOD=$(METHOD) TARGETDB=$(TARGETDB) LOCKTYPE=$(LOCKTYPE) ISOTYPE=$(ISOTYPE) STAGINGDB=$(STAGINGDB)

GO
:SETVAR CONCURRENTCYCLES 1                                                      
GO
!!FOR /L %i IN (1,1,$(CONCURRENTCYCLES)) DO Start SQLCMD -S$(SQLSERVER) -E -d$(DB) -i"$(SCRIPTPATH)$(TESTSCRIPT)" -e -r1 -v LOADMETHOD="$(STYLE)$(METHOD)$(ISOTYPE)$(LOCKTYPE)" CONCURRENTCYCLES=$(CONCURRENTCYCLES) TARGETDB=$(TARGETDB) STAGINGDB=$(STAGINGDB)
GO
:SETVAR CONCURRENTCYCLES 3                                                      
GO
!!FOR /L %i IN (1,1,$(CONCURRENTCYCLES)) DO Start SQLCMD -S$(SQLSERVER) -E -d$(DB) -i"$(SCRIPTPATH)$(TESTSCRIPT)" -e -r1 -v LOADMETHOD="$(STYLE)$(METHOD)$(ISOTYPE)$(LOCKTYPE)" CONCURRENTCYCLES=$(CONCURRENTCYCLES) TARGETDB=$(TARGETDB) STAGINGDB=$(STAGINGDB)
GO
:SETVAR CONCURRENTCYCLES 5                                                      
GO
!!FOR /L %i IN (1,1,$(CONCURRENTCYCLES)) DO Start SQLCMD -S$(SQLSERVER) -E -d$(DB) -i"$(SCRIPTPATH)$(TESTSCRIPT)" -e -r1 -v LOADMETHOD="$(STYLE)$(METHOD)$(ISOTYPE)$(LOCKTYPE)" CONCURRENTCYCLES=$(CONCURRENTCYCLES) TARGETDB=$(TARGETDB) STAGINGDB=$(STAGINGDB)
GO
:SETVAR CONCURRENTCYCLES 10                                                     
GO
!!FOR /L %i IN (1,1,$(CONCURRENTCYCLES)) DO Start SQLCMD -S$(SQLSERVER) -E -d$(DB) -i"$(SCRIPTPATH)$(TESTSCRIPT)" -e -r1 -v LOADMETHOD="$(STYLE)$(METHOD)$(ISOTYPE)$(LOCKTYPE)" CONCURRENTCYCLES=$(CONCURRENTCYCLES) TARGETDB=$(TARGETDB) STAGINGDB=$(STAGINGDB)
GO

SELECT DATEDIFF(s, StartTime, SYSDATETIME()) AS [$(STYLE)$(METHOD)$(ISOTYPE)$(LOCKTYPE) Cycle Duration (seconds)] 
FROM  #CycleTimer;

EXEC $(TARGETDB).dbo.pGetIndexDetails 'SAMPLED';

UPDATE #CycleTimer SET StartTime = SYSDATETIME();

GO
:SETVAR LOCKTYPE "PAG"    -- ROW - use row locks   PAG - use page locks         
GO
-- build the stored procedure
!!SQLCMD -S$(SQLSERVER) -E -d$(TARGETDB) -i"$(SCRIPTPATH)Generate$(STYLE)Proc.sql" -e -r1 -v METHOD="$(METHOD)" TARGETDB="$(TARGETDB)" LOCKTYPE="$(LOCKTYPE)" ISOTYPE="$(ISOTYPE)" STAGINGDB=$(STAGINGDB)

GO
:SETVAR CONCURRENTCYCLES 1                                                      
GO
!!FOR /L %i IN (1,1,$(CONCURRENTCYCLES)) DO Start SQLCMD -S$(SQLSERVER) -E -d$(TARGETDB) -i"$(SCRIPTPATH)$(TESTSCRIPT)" -e -r1 -v LOADMETHOD="$(STYLE)$(METHOD)$(ISOTYPE)$(LOCKTYPE)" CONCURRENTCYCLES=$(CONCURRENTCYCLES) TARGETDB=$(TARGETDB) STAGINGDB=$(STAGINGDB)
GO
:SETVAR CONCURRENTCYCLES 3                                                      
GO
!!FOR /L %i IN (1,1,$(CONCURRENTCYCLES)) DO Start SQLCMD -S$(SQLSERVER) -E -d$(TARGETDB) -i"$(SCRIPTPATH)$(TESTSCRIPT)" -e -r1 -v LOADMETHOD="$(STYLE)$(METHOD)$(ISOTYPE)$(LOCKTYPE)" CONCURRENTCYCLES=$(CONCURRENTCYCLES) TARGETDB=$(TARGETDB) STAGINGDB=$(STAGINGDB)
GO
:SETVAR CONCURRENTCYCLES 5                                                      
GO
!!FOR /L %i IN (1,1,$(CONCURRENTCYCLES)) DO Start SQLCMD -S$(SQLSERVER) -E -d$(TARGETDB) -i"$(SCRIPTPATH)$(TESTSCRIPT)" -e -r1 -v LOADMETHOD="$(STYLE)$(METHOD)$(ISOTYPE)$(LOCKTYPE)" CONCURRENTCYCLES=$(CONCURRENTCYCLES) TARGETDB=$(TARGETDB) STAGINGDB=$(STAGINGDB)
GO
:SETVAR CONCURRENTCYCLES 10                                                      
GO
!!FOR /L %i IN (1,1,$(CONCURRENTCYCLES)) DO Start SQLCMD -S$(SQLSERVER) -E -d$(TARGETDB) -i"$(SCRIPTPATH)$(TESTSCRIPT)" -e -r1 -v LOADMETHOD="$(STYLE)$(METHOD)$(ISOTYPE)$(LOCKTYPE)" CONCURRENTCYCLES=$(CONCURRENTCYCLES) TARGETDB=$(TARGETDB) STAGINGDB=$(STAGINGDB)

GO
SELECT DATEDIFF(s, StartTime, SYSDATETIME()) AS [$(STYLE)$(METHOD)$(ISOTYPE)$(LOCKTYPE) Cycle Duration (microseconds)] 
FROM  #CycleTimer;

EXEC $(TARGETDB).dbo.pGetIndexDetails 'SAMPLED';
