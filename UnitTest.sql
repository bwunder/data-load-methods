:SETVAR SQLSERVER "BILL764\R2"                                                         
:SETVAR DB "staging"                    -- staging  target                             
:SETVAR STYLE "AB"                      -- Row  TVP  AB                                
:SETVAR METHOD "Upsert"                 -- Merge  Upsert                               
:SETVAR LOCKTYPE "ROW"                  -- ROW  PAG  TAB                               
:SETVAR ISOTYPE "VER"                   -- VER  LCK                                    
:SETVAR SCRIPTPATH "C:\Users\bwunder\Documents\SQL Server Management Studio\Projects\data load methods\data load methods\" 
:SETVAR TESTSCRIPT "SQLTestCycle.sql"   -- SQLTestCycle.sql  SSISTestCycle.sql         
:SETVAR CONCURRENTCYCLES 1                                                             
:SETVAR STAGINGDB "staging"             -- staging  target                             
:SETVAR TARGETDB "target"                -- staging  target                             

-- pregenerate proc(s)

EXEC $(STAGINGDB).dbo.pGetStagingRowCounts;
GO

!!FOR /L %i IN (1,1,$(CONCURRENTCYCLES)) DO Start SQLCMD -S$(SQLSERVER) -E -d$(DB) -e -i "$(SCRIPTPATH)$(TESTSCRIPT)" -v LOADMETHOD=$(STYLE)$(METHOD)$(ISOTYPE)$(LOCKTYPE) CONCURRENTCYCLES=$(CONCURRENTCYCLES) TARGETDB=$(TARGETDB) STAGINGDB=$(STAGINGDB)
 
 
GO
EXEC $(STAGINGDB).dbo.pGetStagingRowCounts;
