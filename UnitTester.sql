:SETVAR SQLSERVER "BILL764\ATER2"                                                      
:SETVAR DB "staging"                    -- staging  target                             
:SETVAR STYLE "AB"                      -- Row  TVP  AB                                
:SETVAR METHOD "Upsert"                 -- Merge  Upsert                               
:SETVAR LOCKTYPE "ROW"                  -- ROW  PAG  TAB                               
:SETVAR ISOTYPE "VER"                   -- VER  LCK                                    
:SETVAR SCRIPTPATH "C:\Users\bwunder\Documents\SQL Server Management Studio\Projects\data load methods\data load methods\" 
:SETVAR TESTSCRIPT "SQLTestCycle.sql"   -- SQLTestCycle.sql  SSISTestCycle.sql         
:SETVAR CONCURRENTCYCLES 5              -- careful, can run staging out of log space   
:SETVAR STAGINGDB "staging"             -- staging  target                             
:SETVAR TARGETDB "target"               -- staging  target                             

SET NOCOUNT ON;
GO

USE $(TARGETDB);

TRUNCATE TABLE dbo.tChild; 

DELETE dbo.tParent; 

GO

USE $(STAGINGDB);

IF '$(DB)' = '$(STAGINGDB)'
  BEGIN  
  
    IF ('$(STYLE)' = 'AB')
      EXEC dbo.ABChangeView;
    ELSE
      TRUNCATE TABLE dbo.StagedData;
  END


GO

-- the proc has the escalation strategy embedded in the name 
-- jobs are created but disabled
!!SQLCMD -S$(SQLSERVER) -E -d$(DB) -e -i "$(SCRIPTPATH)$(STYLE)GenerateProc.sql" -v SQLSERVER=$(SQLSERVER) STAGINGDB=$(STAGINGDB) TARGETDB=$(TARGETDB) METHOD=$(METHOD) ISOTYPE=$(ISOTYPE) LOCKTYPE="$(LOCKTYPE)" CONCURRENTCOUNT=$(CONCURRENTCYCLES) 

GO

EXEC $(TARGETDB).dbo.pGetIndexDetails 'SAMPLED';

IF ('$(DB)' = '$(STAGINGDB)')
  EXEC $(STAGINGDB).dbo.pGetStagingRowCounts;

GO
--  runs a single TestCycle with stdout pointed to this SPIDs results window
--!!SQLCMD -S$(SQLSERVER) -E -d$(DB) -e -i "$(SCRIPTPATH)$(TESTSCRIPT)" -v LOADMETHOD="$(STYLE)$(METHOD)$(ISOTYPE)$(LOCKTYPE)" CONCURRENTCYCLES="$(CONCURRENTCYCLES)" TARGETDB="$(TARGETDB)" STAGINGDB="$(STAGINGDB)"

!!FOR /L %i IN (1,1,$(CONCURRENTCYCLES)) DO Start SQLCMD -S$(SQLSERVER) -E -d$(DB) -e -i "$(SCRIPTPATH)$(TESTSCRIPT)" -v LOADMETHOD="$(STYLE)$(METHOD)$(ISOTYPE)$(LOCKTYPE)" CONCURRENTCYCLES="$(CONCURRENTCYCLES)" TARGETDB="$(TARGETDB)" STAGINGDB="$(STAGINGDB)"


  
GO

IF ('$(DB)' = '$(STAGINGDB)') AND ('$(STYLE)' = 'AB')
  BEGIN                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
    EXEC $(STAGINGDB).dbo.pGetStagingRowCounts;
    EXEC $(STAGINGDB).dbo.ABChangeView;
    EXEC $(STAGINGDB).dbo.pGetStagingRowCounts;

    EXEC $(STAGINGDB).dbo.ABView$(METHOD);
  END

EXEC $(TARGETDB).dbo.pGetIndexDetails 'SAMPLED';

GO

