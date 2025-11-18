1.  Merge Adjacent file do not merge two files if they have different data schema.
For example one file was written before schema change and other files was written  after schema change.
2. Deletion ducklake_files_scheduled_for_deletion.schedule_start indicates when its safe to remove a file
3. Process of replacing files would require following 
   1. add a new file to a new table.
   2. Check the column mapping of the two tables. They should be same in ducklake_column_mapping
   3. Need to update the table_id in ducklake_data_file
   4. Need to update columns_id and table_id in ducklake_file_column_stats
   5. Need to move file from ducklake_data_files to ducklake_files_scheduled_for_deletion
   
   
