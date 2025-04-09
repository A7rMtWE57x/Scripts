SELECT size_tb,
       size_gb,                                                                                                                                                      
       --size_mb,--mb,
       owner,
       table_name,
       segment_type,
       tablespace_name,
       lob_column,
       CASE
          WHEN t1.segment_type IN ('TABLE', 'LOBSEGMENT') THEN
             (SELECT t0.num_rows
                FROM dba_tables t0
               WHERE t0.owner = t1.owner AND t0.table_name = t1.table_name)
          WHEN t1.segment_type IN ('INDEX') THEN
             (SELECT t0.num_rows
                FROM dba_indexes t0
               WHERE t0.owner = t1.owner AND t0.index_name = t1.table_name)
       END
          num_rows
  FROM (  SELECT CASE WHEN bytes > POWER (1024, 4) THEN ROUND (bytes / POWER (1024, 4), 2) END size_tb,
                 CASE WHEN bytes BETWEEN POWER (1024, 3) AND POWER (1024, 4) THEN TRUNC (bytes / POWER (1024, 3)) END size_gb,
                 CASE WHEN bytes BETWEEN POWER (1024, 2) AND POWER (1024, 3) THEN TRUNC (bytes / POWER (1024, 2)) END size_mb,
                 ROUND (bytes / 1024 / 1024) mb,
                 owner,
                 CASE
                    WHEN t1.segment_type = 'LOBSEGMENT' THEN
                       (SELECT t0.table_name
                          FROM dba_lobs t0
                         WHERE t0.owner = t1.owner AND t0.segment_name = t1.segment_name)
                    ELSE
                       segment_name
                 END
                    table_name,
                 segment_type,
                 tablespace_name,
                 CASE
                    WHEN t1.segment_type = 'LOBSEGMENT' THEN
                       (SELECT t0.table_name
                          FROM dba_lobs t0
                         WHERE t0.owner = t1.owner AND t0.segment_name = t1.segment_name)
                 END
                    lob_table,
                 CASE
                    WHEN t1.segment_type = 'LOBSEGMENT' THEN
                       (SELECT t0.column_name
                          FROM dba_lobs t0
                         WHERE t0.owner = t1.owner AND t0.segment_name = t1.segment_name)
                 END
                    lob_column
            FROM dba_segments t1
           WHERE bytes > 1024 * 1024 * 1024 AND segment_type != 'TYPE2 UNDO'
        --AND owner NOT IN ('SYS')
        ORDER BY bytes DESC) t1;

  SELECT t0.*
    FROM (SELECT ROUND (bytes / 1024 / 1024) mb,
                 SUM (ROUND (bytes / 1024 / 1024)) OVER () sum_total,
                 SUM (ROUND (bytes / 1024 / 1024)) OVER (PARTITION BY tablespace_name ORDER BY bytes DESC) sum_over_tablespace,
                 ROUND (100 * (SUM (ROUND (bytes / 1024 / 1024)) OVER (PARTITION BY tablespace_name ORDER BY bytes DESC) / SUM (ROUND (bytes / 1024 / 1024)) OVER ())) perc_tab,
                 t1.*
            FROM dba_segments t1
           WHERE tablespace_name NOT IN ('SYSAUX', 'SYSTEM') AND tablespace_name IN ('TBS_DAT')) t0
ORDER BY bytes DESC;


SELECT DECODE (t,  1, '<  1GB',  2, '>  1GB <  2GB',  3, '>  2GB <  5GB',  4, '>  5GB < 10GB',  5, '> 10GB < 50GB',  6, '> 50GB <100GB',  7, '>100GB') t_descr,
       t0.*,
       ROUND (gb / SUM (gb) OVER () * 100, 2) perc_overall
  FROM (  SELECT t, ROUND (SUM (bytes) / 1024 / 1024 / 1024) gb, COUNT (*) nr_objs
            FROM (SELECT CASE
                            WHEN bytes / 1024 / 1024 BETWEEN 0 AND 1024 THEN 1
                            WHEN bytes / 1024 / 1024 BETWEEN 1025 AND 2048 THEN 2
                            WHEN bytes / 1024 / 1024 BETWEEN 2049 AND 5120 THEN 3
                            WHEN bytes / 1024 / 1024 BETWEEN 5121 AND 10240 THEN 4
                            WHEN bytes / 1024 / 1024 BETWEEN 10241 AND 51200 THEN 5
                            WHEN bytes / 1024 / 1024 BETWEEN 51201 AND 102400 THEN 6
                            WHEN bytes / 1024 / 1024 > 102401 THEN 7
                         END
                            t,
                         t1.*
                    FROM dba_segments t1)
        GROUP BY t
        ORDER BY 1) t0;

with 
  indexes_with_secondary as
      ( --
        -- normal indexes
        --
        select owner, index_name, table_name
        from   dba_indexes
        where  table_name not in ( select secondary_object_name from user_secondary_objects)
        union all
        --
        -- secondary tables
        --
        select uso.index_owner, uso.secondary_object_name, ui.table_name
        from   dba_secondary_objects uso,
               dba_tables ut,
               dba_indexes ui
        where  ut.table_name = uso.secondary_object_name
        and    uso.index_name = ui.index_name
        and    uso.index_owner = ut.owner
        and    uso.index_owner = ui.owner
        union all
        --
        -- indexes on secondary tables
        --
        select ut.owner, ui.index_name, ui_parent.table_name
        from   dba_indexes ui,
               dba_secondary_objects uso,
               dba_tables ut,
               dba_indexes ui_parent
        where  ut.table_name = uso.secondary_object_name
        and    ut.table_name = ui.table_name
        and    uso.index_name = ui_parent.index_name
        and    ut.owner = uso.index_owner
        and    ut.owner = ui.owner
        and    uso.index_owner = ui_parent.owner
      ),  
  lobs_with_secondary as
      ( --
        -- normal lobs
        --
        select owner, segment_name, table_name
        from   dba_lobs
        where  (owner, table_name) not in ( select t0.index_owner, t0.secondary_object_name from dba_secondary_objects t0)
        union all
        --
        -- secondary tables
        --
        select ul.owner, ul.segment_name, ut.table_name
        from   dba_lobs ul,
               dba_secondary_objects uso,
               dba_tables ut,
               dba_indexes ui
        where  ul.table_name = uso.secondary_object_name
        and    uso.index_name = ui.index_name
        and    ui.table_name = ut.table_name
        and    ul.owner = uso.index_owner
        and    uso.index_owner = ui.owner
        and    ui.owner  = ut.owner
      ),
  seg_space as
      (
        select s.owner,
          coalesce(
             i.table_name,
             l.table_name,
            case when s.segment_name like 'BIN$%' then '$RECYCLE_BIN$' else s.segment_name end) seg, 
        sum(s.bytes) byt
        from dba_segments s,
             indexes_with_secondary i,
             lobs_with_secondary l
        where s.segment_name = i.index_name(+)
        and   s.segment_name = l.segment_name(+)
        and   s.owner        = i.owner (+)
        and   s.owner        = l.owner (+)
        group by s.owner, coalesce(i.table_name,l.table_name,
            case when s.segment_name like 'BIN$%' then '$RECYCLE_BIN$' else s.segment_name end)
      )
select owner, seg,
        lpad(case
           when byt > 1024*1024*1024 then round(byt/1024/1024/1024)||'G'
           when byt > 1024*1024 then round(byt/1024/1024)||'M'
           else round(byt/1024)||'K'
        end,8) sz
from  seg_space s
order by byt desc;
