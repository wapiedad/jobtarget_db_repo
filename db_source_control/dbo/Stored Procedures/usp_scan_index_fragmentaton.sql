/*
This procedure is intended to capture results of scan density 
this is ttesing only this will be testing only
*/ 
CREATE   PROCEDURE [dbo].[usp_scan_index_fragmentaton]
(
 @TableName sysname = NULL,
 @ScanDensity  tinyint  = 100
)
AS
SET @ScanDensity = 100
IF @ScanDensity NOT BETWEEN 1 AND 100
 BEGIN
  RAISERROR('Value supplied:%i is not valid. @ScanDensity is a percentage. Please supply a value for Scan Density between 1 and 100.', 16, 1, @ScanDensity)
  RETURN
 END
IF @TableName IS NOT NULL
 BEGIN
  IF OBJECTPROPERTY(object_id(@TableName), 'IsUserTable') = 0 
   BEGIN
    RAISERROR('Object: %s exists but is NOT a User-defined Table. This procedure only accepts valid table names to process for index rebuilds.', 16, 1, @TableName)
    RETURN
   END
  ELSE
   BEGIN
    IF OBJECTPROPERTY(object_id(@TableName), 'IsTable') IS NULL
     BEGIN
      RAISERROR('Object: %s does not exist within this database. Please check the table name and location (which database?). This procedure only accepts existing table names to process for index rebuilds.', 16, 1, @TableName)
      RETURN
     END
   END
    END--Create a temp table to hold results 
CREATE TABLE #ShowContigOutput
(
 ObjectName  sysname,
 ObjectId  int,
 IndexName  sysname,
 IndexId   tinyint,
 [Level]   tinyint,
 Pages   int,
 [Rows]   bigint,
 MinimumRecordSize smallint,
 MaximumRecordSize smallint,
 AverageRecordSize smallint,
 ForwardedRecords bigint,
 Extents   int,
 ExtentSwitches  numeric(10,2),
 AverageFreeBytes numeric(10,2),
 AveragePageDensity numeric(10,2),
 ScanDensity  numeric(10,2),
 BestCount  int,
 ActualCount  int,
 LogicalFragmentation numeric(10,2),
 ExtentFragmentation numeric(10,2)
)                         IF @TableName IS NOT NULL -- then we only need the showcontig output for that table
 INSERT #ShowContigOutput
  EXEC('DBCC SHOWCONTIG (' + @TableName + ') WITH FAST, ALL_INDEXES, TABLERESULTS') 
ELSE -- All Tables, All Indexes Will be processed.
 INSERT #ShowContigOutput
  EXEC('DBCC SHOWCONTIG WITH FAST, ALL_INDEXES, TABLERESULTS') 
PRINT N' 'DECLARE @ObjectName sysname,
 @IndexName  sysname,
 @QObjectName  nvarchar(258),
 @QIndexName  nvarchar(258),
 @IndexID  tinyint,
 @ActualScanDensity numeric(10,2),
 @InformationalOutput nvarchar(4000),
 @StartTime  datetime,
 @EndTime  datetime
 DECLARE TableIndexList CURSOR FAST_FORWARD FOR 
 SELECT ObjectName, IndexName, IndexID, ScanDensity 
 FROM #ShowContigOutput AS sc
  JOIN sysobjects AS so ON sc.ObjectID = so.id
 WHERE sc.ScanDensity < @ScanDensity 
  AND (OBJECTPROPERTY(sc.ObjectID, 'IsUserTable') = 1 
  OR OBJECTPROPERTY(sc.ObjectID, 'IsView') = 1)
  AND so.STATUS > 0
  AND sc.IndexID BETWEEN 1 AND 250 
  AND sc.ObjectName NOT IN ('dtproperties') 
   -- Here you can list large tables you do not WANT rebuilt.
 ORDER BY sc.ObjectName, sc.IndexID
 OPEN TableIndexList
 FETCH NEXT FROM TableIndexList 
 INTO @ObjectName, @IndexName, @IndexID, @ActualScanDensity
-- WHILE (@@fetch_status <> -1
 SELECT ObjectName, IndexName, IndexID, ScanDensity 
 FROM #ShowContigOutput where scandensity<70 and objectname not like 'sys%' order by objectname asc
/*BEGIN
 IF (@@fetch_status <> -2)
 BEGIN
  SELECT @QObjectName = QUOTENAME(@ObjectName, ']')
  SELECT @QIndexName = QUOTENAME(@IndexName, ']')
  SELECT @InformationalOutput = N'Processing Table: ' + RTRIM(UPPER(@QObjectName)) 
         + N' Rebuilding Index: ' + RTRIM(UPPER(@QIndexName))
  PRINT @InformationalOutput
  IF @IndexID = 1 
  BEGIN
   SELECT @StartTime = getdate()
   EXEC sp_RebuildClusteredIndex_And_IndexedViews @ObjectName, @IndexName
   SELECT @EndTime = getdate()
   SELECT @InformationalOutput = N'Total Time to process = ' + convert(nvarchar, datediff(ms, @StartTime, @EndTime)) + N' ms, finished at ' + (convert(nvarchar, getdate(), 120))    PRINT @InformationalOutput 
  END
  ELSE
  BEGIN
   SELECT @StartTime = getdate()
   EXEC('DBCC DBREINDEX(' + @QObjectName + ', ' + @QIndexName + ') WITH NO_INFOMSGS')
   SELECT @EndTime = getdate()
   SELECT @InformationalOutput = N'Total Time to process = ' + convert(nvarchar, datediff(ms, @StartTime, @EndTime)) + N' ms, finished at ' + (convert(nvarchar, getdate(), 120)) -- see above
   PRINT @InformationalOutput 
  END
  PRINT N' '
  FETCH NEXT FROM TableIndexList 
   INTO @ObjectName, @IndexName, @IndexID, @ActualScanDensity
 END
END
PRINT N' '
SELECT @InformationalOutput = N'***** All Indexes have been rebuilt.  ***** ' 
PRINT @InformationalOutput 
DEALLOCATE TableIndexList
*/