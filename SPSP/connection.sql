SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE state <> 'idle'
  AND pid <> pg_backend_pid()
  -- 可根据需要添加额外过滤条件：
  -- AND datname = 'your_database'
;
