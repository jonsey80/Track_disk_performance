use [DATABASE];

create table dbo.MI_IO_Table(
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
[Avg_KBs_Transfer]  int null,
[Record_time] datetime2(7) null default getdate() 
)