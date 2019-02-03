import threading
import time
import calendar
import psycopg2
import logging

from powa_remote.snapshot import (get_snapshot_functions, powa_databases_src,
                                  powa_statements_src, powa_user_functions_src,
                                  powa_all_relations_src, powa_kcache_src,
                                  powa_qualstats_src, powa_wait_sampling_src)


class PowaThread (threading.Thread):
    def __init__(self, name, repository, config):
        threading.Thread.__init__(self)
        self.__stopping = threading.Event()
        self.__got_sighup = threading.Event()
        self.__connected = threading.Event()
        self.name = name
        self.__repository = repository
        self.__config = config
        self.__pending_config = None
        self.__remote_conn = None
        self.__repo_conn = None
        self.logger = logging.getLogger("powa-remote")
        self.last_time = None

        extra = {'threadname': self.name}
        self.logger = logging.LoggerAdapter(self.logger, extra)

        self.logger.debug("Creating worker %s: %r" % (name, config))

    def __repr__(self):
        return ("%s: %s" % (self.name, self.__config["dsn"]))

    def __check_powa(self):
        if (self.__remote_conn is not None):
            cur = self.__remote_conn.cursor()
            cur.execute("SELECT COUNT(*) FROM pg_extension WHERE extname = 'powa'")
            res = cur.fetchone()
            cur.close()

            if (res[0] != 1):
                self.logger.error("PoWA extension not found")
                self.__disconnect_all()
                self.__stopping.set()

    def __reload(self):
        self.logger.info("Reloading configuration")
        self.__config = self.__pending_config
        self.__pending_config = None
        self.__disconnect_all()
        self.__connect()
        self.__got_sighup.clear()

    def __connect(self):
        if ('dsn' not in self.__repository or 'dsn' not in self.__config):
            self.logger.error("Missing connection info")
            self.__stopping.set()
            return

        try:
            if (self.__repo_conn is None):
                self.logger.debug("Connecting on repository...")
                self.__repo_conn = psycopg2.connect(self.__repository['dsn'])
                self.logger.debug("Connected.")
                cur = self.__repo_conn.cursor()
                cur.execute("SET application_name = %s",
                            ('PoWA remote - repo_conn for worker ' + self.name,))
                cur.close()
                self.__repo_conn.commit()

            if (self.__remote_conn is None):
                self.logger.debug("Connecting on remote database...")
                self.__remote_conn = psycopg2.connect(**self.__config['dsn'])
                self.logger.debug("Connected.")
                cur = self.__remote_conn.cursor()
                cur.execute("SET application_name = %s",
                            ('PoWA remote - worker ' + self.name,))
                cur.close()
                self.__remote_conn.commit()

                self.__connected.set()
        except psycopg2.Error as e:
            self.logger.error("Error connecting:\n%s" % e)
            self.__disconnect_all()
            self.__stopping.set()

    def __disconnect_all(self):
        if (self.__remote_conn is not None):
            self.logger.info("Disconnecting from remote server")
            self.__remote_conn.close()
            self.__remote_conn = None
        if (self.__remote_conn is not None):
            self.logger.info("Disconnecting from repository")
            self.__disconnect_repo()
        self.__connected.clear()

    def __disconnect_repo(self):
        if (self.__repo_conn is not None):
            self.__repo_conn.close()
            self.__repo_conn = None

    def __disconnect_all_and_exit(self):
        self.__disconnect_all()
        self.logger.info("stopped")
        self.__stopping.clear()

    def __worker_main(self):
        self.last_time = calendar.timegm(time.gmtime())
        self.__check_powa()
        first_snapshot_taken = False
        while (not self.__stopping.isSet()):
            cur_time = calendar.timegm(time.gmtime())
            if (self.__got_sighup.isSet()):
                self.__reload()

            if ((not first_snapshot_taken) or
                    (cur_time - self.last_time) >= self.__config["frequency"]):
                self.__take_snapshot()
                self.last_time = calendar.timegm(time.gmtime())
                first_snapshot_taken = True
            time.sleep(0.1)

        self.__disconnect_all_and_exit()

    def __take_snapshot(self):
        """
        Main part of the worker thread.  This function will call all the
        query_src functions enabled for the target server, and insert all the
        retrieved rows on the repository server, in unlogged tables, and
        finally call powa_take_snapshot() on the repository server to finish
        the distant snapshot.  All is done in one transaction, so that there
        won't be concurrency issues if a snapshot takes longer than the
        specified interval.
        """
        srvid = self.__config["srvid"]

        self.__connect()

        # get the list of snapshot functions, and their associated query_src
        cur = self.__remote_conn.cursor()
        cur.execute(get_snapshot_functions(), (srvid,))
        snapfuncs = cur.fetchall()
        cur.close()

        for snapfunc in snapfuncs:
            ins_sql = None

            # get the SQL needed to insert the query_src data on the remote
            # server into the transient unlogged table on the repository server
            if (snapfunc[0] is None):
                self.logger.warn("Not query_source for %s" % snapfunc[1])
                continue
            elif (snapfunc[0] == "powa_statements_src"):
                ins_sql = powa_statements_src()
            elif (snapfunc[0] == "powa_databases_src"):
                ins_sql = powa_databases_src()
            elif (snapfunc[0] == "powa_user_functions_src"):
                ins_sql = powa_user_functions_src()
            elif (snapfunc[0] == "powa_all_relations_src"):
                ins_sql = powa_all_relations_src()
            elif (snapfunc[0] == "powa_kcache_src"):
                ins_sql = powa_kcache_src()
            elif (snapfunc[0] == "powa_qualstats_src"):
                ins_sql = powa_qualstats_src()
            elif (snapfunc[0] == "powa_wait_sampling_src"):
                ins_sql = powa_wait_sampling_src()
            else:
                self.logger.warn("Unhandled %s" % snapfunc[1])
                continue

            ins = self.__repo_conn.cursor()
            data_src = self.__remote_conn.cursor()

            # execute the query_src functions to get local data (srvid 0)
            self.logger.debug("Calling %s(0)..." % snapfunc[0])
            data_src_sql = ("SELECT %(srvid)d, * FROM %(fname)s(0)" %
                   {'fname': snapfunc[0], 'srvid': srvid})

            # use savepoint, maybe the datasource is not setup on the remote
            # server
            data_src.execute("SAVEPOINT src")
            try:
                data_src.execute(data_src_sql)
            except psycopg2.Error as e:
                self.logger.warn("Error while calling %s:\n%s" % (snapfunc[0],
                                                                  e))
                data_src.execute("ROLLBACK TO src")

            # insert the data to the transient unlogged table
            row = data_src.fetchone()
            ins.execute("SAVEPOINT data")
            while (row is not None):
                try:
                    # FIXME use COPY instead
                    ins.execute(ins_sql, row)
                    row = data_src.fetchone()
                # if we hit an error while inserting the data on the repository
                # server, rollback all data that may have been inserted for
                # this datasource and give up with this data source
                except psycopg2.Error as e:
                    self.logger.warn("Error while inserting data:\n%s" % e)
                    self.logger.warn("Giving up for %s", snapfunc[1])
                    ins.execute("ROLLBACK TO data")
                    row = None

        # call powa_take_snapshot() for the given server
        self.logger.debug("Calling powa_take_snapshot(%d)..." % (srvid))
        sql = ("SELECT powa_take_snapshot(%(srvid)d)" % {'srvid': srvid})
        ins.execute(sql)
        val = ins.fetchone()[0]
        if (val != 0):
            self.logger.warning("Number of errors during snapshot: %d", val)

        ins.execute("SET application_name = %s",
                    ('PoWA remote - repo_conn for worker ' + self.name,))
        ins.close()

        # and finally commit the transaction
        self.logger.debug("Committing transaction")
        self.__repo_conn.commit()
        self.__remote_conn.commit()

        self.__disconnect_repo()

    def is_stopping(self):
        return self.__stopping.isSet()

    def get_config(self):
        return self.__config

    def ask_to_stop(self):
        self.__stopping.set()
        self.logger.info("Asked to stop...")

    def run(self):
        if (not self.__stopping.isSet()):
            self.logger.info("Starting worker")
            self.__worker_main()

    def ask_reload(self, new_config):
        self.logger.debug("Reload asked")
        self.__pending_config = new_config
        self.__got_sighup.set()

