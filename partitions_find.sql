-- for greenplum databases
SELECT p.schemaname, p.tablename,
p.partitionname, p.partitionboundary,
c.columnname
FROM pg_partitions p
JOIN pg_partition_columns c ON p.tablename = c.tablename
WHERE p.tablename = 'name_of_your_table'