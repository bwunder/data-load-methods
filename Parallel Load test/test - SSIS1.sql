:SETVAR LOADMETHOD "TVPCmdProc"     -- RowAdapter RowCmdProc RowLINQ TVPAdapter TVPCmdProc                           
:SETVAR TARGETDB "target"                                                                                            
:SETVAR PACKAGEPATH "C:\Users\bwunder\Documents\Visual Studio 2008\Projects\data load packages\data load packages\"  
:SETVAR ROWSTOPUSH "/SET \Package.Variables[User::RowsToPush].Properties[Value];" -- starts with /SET and ends on ;  
-- for RowAdapter only, ALWAYS set to "" for other methods
:SETVAR BATCHUPDATES "" --"/SET \Package.Variables[User::BatchUpdates].Properties[Value];true" -- true or false     
-- use to limit the output verbosity and SSIS checkpointing insanity (I guess)
:SETVAR DTEXECPARMS "/CHECKPOINTING OFF /REPORTING EW"                                                               

SET NOCOUNT ON
GO
USE $(TARGETDB);
EXEC dbo.pGetIndexDetails 'SAMPLED';
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)10000 $(BATCHUPDATES) $(DTEXECPARMS)      
GO    
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)1 $(BATCHUPDATES) $(DTEXECPARMS)          
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)1 $(BATCHUPDATES) $(DTEXECPARMS)          
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)5 $(BATCHUPDATES) $(DTEXECPARMS)          
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)5 $(BATCHUPDATES) $(DTEXECPARMS)          
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)10 $(BATCHUPDATES) $(DTEXECPARMS)         
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)10 $(BATCHUPDATES) $(DTEXECPARMS)         
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)50 $(BATCHUPDATES) $(DTEXECPARMS)         
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)50 $(BATCHUPDATES) $(DTEXECPARMS)         
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)100 $(BATCHUPDATES) $(DTEXECPARMS)        
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)100 $(BATCHUPDATES) $(DTEXECPARMS)        
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)500 $(BATCHUPDATES) $(DTEXECPARMS)        
GO 
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)500 $(BATCHUPDATES) $(DTEXECPARMS)        
GO 
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)1000 $(BATCHUPDATES) $(DTEXECPARMS)       
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)1000 $(BATCHUPDATES) $(DTEXECPARMS)       
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)5000 $(BATCHUPDATES) $(DTEXECPARMS)       
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)10000 $(BATCHUPDATES) $(DTEXECPARMS)      
GO    
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)50000 $(BATCHUPDATES) $(DTEXECPARMS)      
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)100000 $(BATCHUPDATES) $(DTEXECPARMS)     
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)10000 $(BATCHUPDATES) $(DTEXECPARMS)      
GO    
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)5000 $(BATCHUPDATES) $(DTEXECPARMS)       
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)1000 $(BATCHUPDATES) $(DTEXECPARMS)       
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)1000 $(BATCHUPDATES) $(DTEXECPARMS)       
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)500 $(BATCHUPDATES) $(DTEXECPARMS)        
GO 
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)500 $(BATCHUPDATES) $(DTEXECPARMS)        
GO 
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)100 $(BATCHUPDATES) $(DTEXECPARMS)        
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)100 $(BATCHUPDATES) $(DTEXECPARMS)        
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)50 $(BATCHUPDATES) $(DTEXECPARMS)         
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)50 $(BATCHUPDATES) $(DTEXECPARMS)         
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)10 $(BATCHUPDATES) $(DTEXECPARMS)         
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)10 $(BATCHUPDATES) $(DTEXECPARMS)         
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)5 $(BATCHUPDATES) $(DTEXECPARMS)          
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)5 $(BATCHUPDATES) $(DTEXECPARMS)          
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)1 $(BATCHUPDATES) $(DTEXECPARMS)          
GO
:!!dtexec /FILE "$(PACKAGEPATH)$(LOADMETHOD).dtsx" $(ROWSTOPUSH)1 $(BATCHUPDATES) $(DTEXECPARMS)          
GO

EXEC dbo.pGetActivityForLoadMethod '$(LOADMETHOD)'
EXEC dbo.pGetIndexDetails 'SAMPLED';
GO 
USE master;
