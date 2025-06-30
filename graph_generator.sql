-- COPY te(fid, tid)
-- FROM 'dblp_clean.csv'
-- DELIMITER ','  -- 逗号分隔
-- CSV;
copy edges(source, target) FROM '/Users/chenliangqi/Desktop/SP/dblp_clean.csv' DELIMITER ',' CSV;
