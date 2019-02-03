# XXX should we ignore entries without query_src?
def get_snapshot_functions():
    return """SELECT query_source, function_name
              FROM powa_functions
              WHERE operation = 'snapshot' AND enabled
              AND srvid = %s
              ORDER BY priority"""


def powa_databases_src():
    return """
            INSERT INTO powa_databases_src_tmp(
             srvid, oid, datname
            )
            VALUES (
             %s, -- srvid
             %s, -- oid
             %s --  datname
            )
            """


def powa_statements_src():
    return """
           INSERT INTO powa_statements_src_tmp(
            srvid, ts, userid, dbid, queryid, query, calls,
            total_time, rows, shared_blks_hit, shared_blks_read,
            shared_blks_dirtied, shared_blks_written,
            local_blks_hit, local_blks_read, local_blks_dirtied,
            local_blks_written, temp_blks_read, temp_blks_written,
            blk_read_time, blk_write_time
           )
           VALUES (
            %s, -- srvid
            %s, -- ts
            %s, -- userid
            %s, -- dbid
            %s, -- queryid
            %s, -- query
            %s, -- calls
            %s, -- total_time
            %s, -- rows
            %s, -- shared_blks_hit
            %s, -- shared_blks_read
            %s, -- shared_blks_dirtied
            %s, -- shared_blks_written
            %s, -- local_blks_hit
            %s, -- local_blks_read
            %s, -- local_blks_dirtied
            %s, -- local_blks_written
            %s, -- temp_blks_read
            %s, -- temp_blks_written
            %s, -- blk_read_time
            %s  -- blk_write_time
           )
                """


def powa_user_functions_src():
    return """
        INSERT INTO powa_user_functions_src_tmp(
         srvid, dbid, funcid, calls, total_time, self_time
        )
        VALUES (
         %s, --srvid
         %s, -- ts
         %s, --dbid
         %s, --funcid
         %s, --calls
         %s, --total_time
         %s  --self_time
        )
    """


def powa_all_relations_src():
    return """
        INSERT INTO powa_all_relations_src_tmp(
         srvid, ts, dbid, relid, numscan, tup_returned, tup_fetched, n_tup_ins,
         n_tup_upd, n_tup_del, n_tup_hot_upd, n_liv_tup, n_dead_tup,
         n_mod_since_analyze, blks_read, blks_hit, last_vacuum, vacuum_count,
         last_autovacuum, autovacuum_count, last_analyze, analyze_count,
         last_autoanalyze, autoanalyze_count
        )
        VALUES (
         %s, -- srvid
         %s, -- ts
         %s, -- dbid
         %s, -- relid
         %s, -- numscan
         %s, -- tup_returned
         %s, -- tup_fetched
         %s, -- n_tup_ins
         %s, -- n_tup_upd
         %s, -- n_tup_del
         %s, -- n_tup_hot_upd
         %s, -- n_liv_tup
         %s, -- n_dead_tup
         %s, -- n_mod_since_analyze
         %s, -- blks_read
         %s, -- blks_hit
         %s, -- last_vacuum
         %s, -- vacuum_count
         %s, -- last_autovacuum
         %s, -- autovacuum_count
         %s, -- last_analyze
         %s, -- analyze_count
         %s, -- last_autoanalyze
         %s  -- autoanalyze_count
        )
    """


def powa_kcache_src():
    return """
        INSERT INTO powa_kcache_src_tmp(
         srvid, ts, queryid, userid, dbid, reads, writes, user_time,
         system_time
        )
        VALUES (
         %s, -- srvid,
         %s, -- ts,
         %s, -- queryid,
         %s, -- userid,
         %s, -- dbid,
         %s, -- reads,
         %s, -- writes,
         %s, -- user_time,
         %s  -- system_time
        )
    """


def powa_qualstats_src():
    return """
        INSERT INTO powa_qualstats_src_tmp(
         srvid, ts, uniquequalnodeid, dbid, userid, qualnodeid, occurences,
         execution_count, nbfiltered, queryid, constvalues, quals
        )
        VALUES (
         %s, -- srvid
         %s, -- ts
         %s, -- uniquequalnodeid
         %s, -- dbid
         %s, -- userid
         %s, -- qualnodeid
         %s, -- occurences
         %s, -- execution_count
         %s, -- nbfiltered
         %s, -- queryid
         %s, -- constvalues
         %s  -- quals
        )
    """


def powa_wait_sampling_src():
    return """
        INSERT INTO powa_wait_sampling_src_tmp(
         srvid, ts, dbid, event_type, event, queryid, count
        )
        VALUES (
         %s, -- srvid
         %s, -- ts
         %s, -- dbid
         %s, -- event_type
         %s, -- event
         %s, -- queryid
         %s  -- count
        )
    """
