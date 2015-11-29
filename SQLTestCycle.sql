--:SETVAR LOADMETHOD "?"
--:SETVAR CONCURRENTCYCLES 1

--USE target;

GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 10000, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 1, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 1, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 5, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 5, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 10, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 10, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 50, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 50, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 100, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 100, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 500, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 500, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 1000, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 1000, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 5000, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 10000, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 50000, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 100000, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 10000, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 5000, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 1000, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 1000, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 500, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 500, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 100, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 100, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 50, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 50, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 10, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 10, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 5, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 5, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 1, @ConcurrentCycles = $(CONCURRENTCYCLES);
GO
EXEC dbo.$(LOADMETHOD) @RowsToPush = 1, @ConcurrentCycles = $(CONCURRENTCYCLES);