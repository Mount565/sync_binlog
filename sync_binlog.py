#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import os
import logging
import time
import sys

logging.basicConfig(filename="sync_binlog.log", format="%(asctime)s %(message)s", level=logging.DEBUG)
master_os_user = "mysqlfo"
master_ip = sys.argv[1]
master_binlog_path = "/dbfiles/mysql_home/logs"
master_binlog_index = "/dbfiles/mysql_home/logs/binlog.index"

# The path where master's binlog.index and latest binlog file will be downloaded and saved
tmp_path = "/dbfiles/tmp"

# the slave that is going to be chosen as new master

# candidate_host="172.40.0.163"
candidate_host = sys.argv[3]
candidate_port = sys.argv[4]
# This user must have enough privileges to apply binlog on the candidate slave
candidate_mysql_user = "fo"
candidate_mysql_password = "fo"

candidate_slave = {
    'host': candidate_host,
    'port': int(candidate_port),
    'user': candidate_mysql_user,
    'passwd': candidate_mysql_password,
    'charset': 'utf8'
}

# download master's binlog.index

r = os.system("scp %s@%s:%s %s" % (master_os_user, master_ip, master_binlog_index, tmp_path))
if r == 0:
    logging.info("Download binlog.index successfully...")
else:
    logging.error("Download binlog.index failed, can not sync binlog before fail over!")
    exit(1)

binlog_index = tmp_path + "/binlog.index"

with open(binlog_index, 'r') as f:
    lines = f.readlines();
    latest_line = lines[-1]

# if log_bin=/dbfiles/mysql_home/logs/binlog.log instead of log_bin=binlog.log
# there's no need to extract the latest binlog file
if "/" in latest_line:
    tmp = latest_line.split('/');
    latest_binlog = tmp[-1].strip()

latest_binlog_file = master_binlog_path + "/" + latest_binlog
print latest_binlog_file
print "scp %s@%s:%s %s" % (master_os_user, master_ip, latest_binlog_file, tmp_path)

# download the latest binlog file
r = os.system("scp %s@%s:%s %s" % (master_os_user, master_ip, latest_binlog_file, tmp_path))
if r == 0:
    logging.info("Download latest binlog successfully -->" + latest_binlog)
else:
    logging.error("Failed to download the latest binlog file, can not sync binlog before fail over!")
    exit(1)

log_plain_file = tmp_path + '/' + latest_binlog + ".txt"
# find the last end_log_position in the latest binlog file
os.system("mysqlbinlog " + tmp_path + '/' + latest_binlog + " | tail -100  > " + log_plain_file)


# sp = os.popen("cat " + tmp_path + '/' + latest_binlog + ".txt" )


def __get_latest_log_position(binlogfile):
    with open(binlogfile, 'r') as f:
        lines = f.readlines()
        # reversely iterate the array lines and find the last end_log_pos
        # the binlog format is like:
        # 170626 16:27:59 server id 162  end_log_pos 749 CRC32 0x80dee5b8 	Xid = 552616
        for i in range(0, len(lines) - 1)[::-1]:
            if "end_log_pos" in lines[i]:
                m = lines[i].index("end_log_pos")
                m = m + 12
                n = lines[i].index(" ", m)
                latest_position = lines[i][m:n]
                break;
    return latest_position


latest_log_position = __get_latest_log_position(log_plain_file)
logging.info("The latest log position in the master's binlog file is:" + latest_log_position)

# print latest_log_position
# exit(0)

# connect to candidate slave
try:

    # here is a little tricky

    while True:
        # A little tricky here .
        # As before the script is executed, mysqlfailover already made the candidate most up to date by connecting it to other slaves.
        # In this case , the candidate's slave configuration is changed.
        conn = mdb.connect(**candidate_slave)
        cursor = conn.cursor(cursorclass=mdb.cursors.DictCursor)
        cursor.execute('SHOW SLAVE STATUS;')
        result = cursor.fetchone()
        master_host = result["Master_Host"];
        if master_host != master_ip:
            logging.info(
                "This candidate(" + candidate_host + ")'s slave configuration has changed, try to obtain original slave status from slave(" + master_host + ")")
            candidate_slave = {
                'host': master_host,
                'port': int(candidate_port),
                'user': candidate_mysql_user,
                'passwd': candidate_mysql_password,
                'charset': 'utf8'
            }
        else:
            break;

    master_log_file = result["Master_Log_File"]
    read_master_log_pos = result["Read_Master_Log_Pos"]
    exec_master_log_pos = result["Exec_Master_Log_Pos"]
    slave_sql_running = result["Slave_SQL_Running"]
    logging.info("Slave status==> Master_Log_File:" + master_log_file + ",Read_Master_Log_Pos:" + str(
        read_master_log_pos) + ",Exec_Master_Log_Pos:" + str(exec_master_log_pos) + ",Slave_SQL_Running:" + str(
        slave_sql_running))

    if slave_sql_running != 'Yes' and read_master_log_pos != exec_master_log_pos:
        logging.error(
            "Can't sync master's binlog as slave sql thread stops running and sql thread is still behind io thread. ")
        exit(1)

    # print "master_log_file:" + master_log_file
    # exit(0)
    if master_log_file != latest_binlog:
        logging.error(
            "The candidate's slave reading  master_log_file :" + master_log_file + ", master's newest binlog file:" + latest_binlog)
        logging.error(
            "This candidate(" + candidate_host + ") may be too far behind from master, need to be handled manually!")
        exit(1)
    elif long(latest_log_position) == read_master_log_pos:
        logging.info("This candidate(" + candidate_host + ") is 0 position behind the master, no need to sync binlog")
        exit(0)
    else:
        # check if exec_master_log_pos equals read_master_log_pos ,
        # this is important, as binlog needs to be applied on slave in time sequence .
        while True:
            if exec_master_log_pos != read_master_log_pos:
                logging.info(
                    "exec_master_log_pos(" + str(exec_master_log_pos) + ") is behind read_master_log_pos(" + str(
                        read_master_log_pos) + "), wait 2s to continue")
                time.sleep(2)
            else:
                break
        logging.info("latest log position in master binlog file:" + str(
            latest_log_position) + " <--> read_master_log_pos in slave:" + str(read_master_log_pos))
        logging.info("begin sync binlog to candidate master " + candidate_host + " ...")
        # apply master binlog to this slave
        # if the candidate slave is not behind the master , the following applies nothing, as the read_master_log_pos is the last end log position in the master's binlog file.
        r = os.system('mysqlbinlog  --start-position=' + str(
            read_master_log_pos) + '  ' + tmp_path + '/' + latest_binlog + " | mysql -h" + candidate_host + " -u" + candidate_mysql_user + " -p" + candidate_mysql_password)
        if r == 0:
            logging.info("Master binlog applied on candidate(" + candidate_host + ") successfully!")
        else:
            logging.info("running myqlbinlog failed when try to apply binlog on candidate(" + candidate_host + ")!")
            exit(1)

except Exception as e:
    logging.error(e)
    logging.error("Sync binlog failed!")
    exit(1)
finally:
    cursor.close()
    conn.close()

exit(0)
