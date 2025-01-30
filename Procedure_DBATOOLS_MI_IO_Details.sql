----------------------------------------------------------------------
--Date: 30/01/2025                                                  --
--ver: 1.0														    --
--Author: M.Jones													--
--Details: Log the Latency and stall details of each MI data file   --
--         and the IOPS bucket for Gen 1 at time of creation used   --
--         to monitor the io details to see if more IOPS are required-
----------------------------------------------------------------------
--          Version Control                                         --
----------------------------------------------------------------------
-- Date     Ver   Author				Details                     --
----------------------------------------------------------------------
--30.01.25  1.0   M Jones         Initial Version                   --
----------------------------------------------------------------------


use [DB];
Go

Create procedure [dbo].[MI_IO_Details] as 
begin
	begin try 

				drop table if exists #IO_Table
				drop table if exists #iops_Bucket
				Create table #IOPS_Bucket (
				[Bucket]	varchar(100)	null,
				[Min_fileSize] [bigint] null,
				[max_fileSize] [bigint] null,
				[IOPS]	[bigint] null,
				[Throughput(MiB/s)] [int] null
				)

				create table #IO_Table(
				[DatabaseName] varchar(150)	null,
				[FileName] varchar(100)	null,
				[File_type] varchar(30)	null,
				[Curent_file_size(MB)] numeric(19,2) null,
				[Current_IOPS_bucket] varchar(100) null,
				[CurrentIOPS] int	null,
				[Total_IOP_Since_reboot] bigint null,
				[Avg_IOPS_since_reboot_inc_inavtivetime] int  null,
				[io_stall]	bigint null,
				[Read_stall(S)] bigint null,
				[write_stall(S)]	bigint null,
				[Readlatency]	int null,
				[writelatency] int null,
				[AvgLatency] int null,
				[LatencyAssessment] varchar(100) null,
				[Avg_KBs_Transfer]  int null
				)

				insert into #iops_bucket
				values 
				('>=0 and <=129 GiB', 0, 129, 500, 100),
				('>129 and <=513 GiB', 130,513,2300,150),
				('>513 and <=1025 GiB', 514,1025,5000,200),
				('>1025 and <=2049 GiB',1026,2049,7500,250),
				('>2049 and <=4097 GiB',2050,4097,7500,250),
				('>4097 GiB and <=8 TiB',4098,80000000000,7500,250)

				insert into #IO_Table(
				[DatabaseName],
				[FileName],
				[File_type],
				[Curent_file_size(MB)],
				[Current_IOPS_bucket],
				[CurrentIOPS],
				[Total_IOP_Since_reboot],
				[Avg_IOPS_since_reboot_inc_inavtivetime],
				[io_stall],
				[Read_stall(S)],
				[write_stall(S)] )
				Select [DatabaseName],
					   [FileName],
					   [File_type],
					   [Current_file_size (MB)],
					   [bucket] 'Current_IOPS_bucket',
					   [IOPS] 'Current_IOPS',
					   num_of_reads + Num_of_writes 'Total_IOP_Since_reboot',
					   (num_of_reads + Num_of_writes)/(sample_ms/1000) 'Avg_IOPS_since_reboot_inc_inavtivetime',
					   io_stall,
					   io_stall_read_ms/1000 'Read_stall (s)',
					   io_stall_write_ms 'Write_stall (w)'
				From (	select dbl.name 'DatabaseName',
							   dbl.database_id,
							   masf.name 'FileName',
							   masf.file_id,
							   masf.type_desc 'File_type',
							   ((convert(Numeric(19,2),masf.size)*8)/1024) 'Current_file_size (MB)'	   
						from sys.databases dbl
						inner join sys.master_files masf on dbl.database_id = masf.database_id
						where dbl.database_id > 4 ) t
				inner join #IOPS_Bucket on [Current_file_size (MB)]/1024 > [Min_fileSize] and [Current_file_size (MB)]/1024 < [Max_fileSize]
				cross apply sys.dm_io_virtual_file_stats(t.database_id,t.file_id) ivfs
				where File_type = 'Rows'

				drop table if exists #datafiles

				create table #datafiles (
				[DBID]  int  null,
				[database] varchar(100) null,
				[fileid] int null,
				[filetype] varchar(100) null,
				[spaceallocatedMB] numeric(18,2) null,
				[unusuedspaceMB] numeric(18,2) null,
				[storage_free_mb] numeric(18,2) null,
				[Storage_used] numeric(18,2) null
				)

				EXEC sp_MSforeachdb '
				use ?
				insert into #datafiles
				select *  from (

				SELECT DB_ID() ''DB_ID'',''?'' ''Database'',file_id,''Data File'' as File_type, SUM(size/128.0) AS DatabaseDataSpaceAllocatedInMB,
				SUM(size/128.0 - CAST(FILEPROPERTY(name, ''SpaceUsed'') AS int)/128.0) AS DatabaseDataSpaceAllocatedUnusedInMB,
				(SELECT TOP 1 reserved_storage_mb - storage_space_used_mb
					   FROM master.sys.server_resource_stats
					   ORDER BY end_time DESC) as storage_free_mb,
				(SELECT TOP 1 CAST( (storage_space_used_mb * 100. / reserved_storage_mb) as DECIMAL(9,2)) as [ReservedStoragePercentage]
					   FROM master.sys.server_resource_stats
					   ORDER BY end_time DESC) as ''storage_used(%)''
				FROM sys.database_files
				GROUP BY type_desc,file_id
				HAVING type_desc = ''ROWS''
				union


				SELECT DB_ID() ''DB_ID'', ''?'' ''Database'',file_id,''Log File'' as File_type, SUM(size/128.0) AS DatabaseDataSpaceAllocatedInMB,
				SUM(size/128.0 - CAST(FILEPROPERTY(name, ''SpaceUsed'') AS int)/128.0) AS DatabaseDataSpaceAllocatedUnusedInMB,
				(SELECT TOP 1 reserved_storage_mb - storage_space_used_mb
					   FROM master.sys.server_resource_stats
					   ORDER BY end_time DESC) as storage_free_mb,
				(SELECT TOP 1 CAST( (storage_space_used_mb * 100. / reserved_storage_mb) as DECIMAL(9,2)) as [ReservedStoragePercentage]
					   FROM master.sys.server_resource_stats
					   ORDER BY end_time DESC) as ''storage_used(%)''

				FROM sys.database_files
				GROUP BY type_desc,file_id
				HAVING type_desc = ''Log'') t ;'

				update #IO_Table 
					set [Readlatency] = b.ReadLatency,
						[writelatency] = b.writelatency,
						[AvgLatency] = b.AvgLatency,
						[LatencyAssessment] = b.LatencyAssessment,
						[Avg_KBs_Transfer] = b.[Avg KBs/Transfer]
				from #IO_Table a
				inner join (

				SELECT   

					   LEFT(DB_NAME (vfs.database_id),32) AS [Database Name],
					   mf.name 'logical_name',
					   (DF.spaceallocatedMB)/1024'File_size(GB)',
					   'Est Disk Performance' = case when (DF.spaceallocatedMB)/1024 < 129 then '500 Iops/100 MiB/s'
													 when (DF.spaceallocatedMB)/1024 between 129 and 513 then '2300 Iops/150 MiB/S'
													 when (DF.spaceallocatedMB)/1024 between 513 and 1025 then '5000 Iops/200 MiB/S'
													 when (DF.spaceallocatedMB)/1024 > 1025  then '7500 Iops/250 MiB/S'
													 ENd ,
					   ReadLatency = CASE WHEN num_of_reads = 0 THEN 0 ELSE (io_stall_read_ms / num_of_reads) END, 
						WriteLatency = CASE WHEN num_of_writes = 0 THEN 0 ELSE (io_stall_write_ms / num_of_writes) END, 
						AvgLatency =  CASE WHEN (num_of_reads = 0 AND num_of_writes = 0) THEN 0 
										ELSE (io_stall / (num_of_reads + num_of_writes)) END,
					   LatencyAssessment = CASE WHEN (num_of_reads = 0 AND num_of_writes = 0) THEN 'No data' ELSE 
							   CASE WHEN (io_stall / (num_of_reads + num_of_writes)) < 2 THEN 'Excellent' 
									WHEN (io_stall / (num_of_reads + num_of_writes)) BETWEEN 2 AND 5 THEN 'Very good' 
									WHEN (io_stall / (num_of_reads + num_of_writes)) BETWEEN 6 AND 15 THEN 'Good' 
									WHEN (io_stall / (num_of_reads + num_of_writes)) BETWEEN 16 AND 100 THEN 'Poor' 
									WHEN (io_stall / (num_of_reads + num_of_writes)) BETWEEN 100 AND 500 THEN  'Bad' 
									ELSE 'Deplorable' END  END, 
						 [Avg KBs/Transfer] =  CASE WHEN (num_of_reads = 0 AND num_of_writes = 0) THEN 0 
									ELSE ((([num_of_bytes_read] + [num_of_bytes_written]) / (num_of_reads + num_of_writes)) / 1024) END
					   FROM sys.dm_io_virtual_file_stats (NULL,NULL) AS vfs  
					   JOIN sys.master_files AS mf ON vfs.database_id = mf.database_id 
					   join #datafiles as DF on mf.database_id = Df.DBID and mf.file_id = df.fileid
						 AND vfs.file_id = mf.file_id 
					   where DBID > 4 
				) b on a.DatabaseName = b.[Database Name] and a.FileName = b.logical_name

				Insert into #IO_Table (
				[DatabaseName],
				[FileName],
				[File_type],
				[Curent_file_size(MB)],
				[Current_IOPS_bucket],
				[CurrentIOPS],
				[Total_IOP_Since_reboot],
				[Avg_IOPS_since_reboot_inc_inavtivetime],
				[io_stall],
				[Read_stall(S)],
				[write_stall(S)],
				[Readlatency],
				[writelatency],
				[AvgLatency],
				[LatencyAssessment],
				[Avg_KBs_Transfer]
				)
				select * from #IO_Table
	end try
begin catch
	throw;
end catch 
end