###########################################################################
#
# Data transfer Service
#
# Application for centos8
#
# Copyright (c) 2022 Adhir Dutta (likhon52@gmail.com)
#
# Download oracle-instantclient11.2-basic-11.2.0.4.0-1.x86_64.rpm
#
# sudo yum localinstall oracle-instantclient11.2-basic-11.2.0.4.0-1.x86_64.rpm
# export LD_LIBRARY_PATH=/usr/lib/oracle/11.2/client64/lib:$LD_LIBRARY_PATH
# sudo  ldconfig
# sudo sh -c "echo /usr/lib/oracle/11.2/client64/lib  > /etc/ld.so.conf.d/oracle-instantclient.conf"
#
# sudo dnf install libnsl
# python3 -m pip install cx_Oracle==7.2.3
###########################################################################


import json
import os, sys
import socket
import time
from collections import namedtuple

import cx_Oracle


class dbDataTunneling():
    # init function
    def __init__(self, conf_obj=None):
        if conf_obj is None:
            print("Configuration is missing!!..")
            return None
        self.__run(conf_obj)

    def __run(self, obj):
        """
        @obj: json object
        # template
        {
            "active":true,
            "comm":
                [
                    {"name":"pulp2", "ip":"10.29.13.222","user":"abbsg","user":"abbsg","sid":"ADVA","port":1521,"post":[],"get":[]},
                    {"name":"nps", "ip":"10.29.12.245","user":"abbsg","user":"abbsg","sid":"ADVA","port":1521,"post":[],"get":[]}
                ]
        }
        """
        # isActive
        if not obj.active:
            return False

        param1 = obj.comm[0]
        param2 = obj.comm[1]

        # Check Network status
        flag_1 = self.__isNetworkActive(param1.ip, param1.port)
        if not flag_1:
            print("{} Network is Down!".format(param1.name))
            return False
        flag_2 = self.__isNetworkActive(param2.ip, param2.port)        
        if not flag_2:           
            print("{} Network is Down!".format(param2.name))
            return False

        conn1 = self.__checkDbConn(
            param1.ip, param1.user, param1.passwd, param1.port, param1.sid)
        if conn1 is None:
            # print(f"{param1.name} Database Connection failed!")
            print("{} Database Connection failed!".format(param1.name))
            return False

        conn2 = self.__checkDbConn(
            param2.ip, param2.user, param2.passwd, param2.port, param2.sid)
        if conn2 is None:
            # print(f"{param2.name} Database Connection failed!")
            print("{} Database Connection failed!".format(param2.name))
            return False

        # list of tables for conn1 (post and get)
        list_conn1_send_tables = param1.send
        list_conn2_recv_tables = param2.receive
        c1_flag = self.__send_data(
            conn1, conn2, list_conn1_send_tables, list_conn2_recv_tables)
        if c1_flag:
            # print(f"...>> Data sending from {param1.name} to {param2.name} ..>> Done!!")
            print("...>> Data sending from {a} to {b} ..>> Done!!".format(a=param1.name, b=param2.name))

        list_conn2_send_tables = param2.send
        list_conn1_recv_tables = param1.receive
        c2_flag = self.__send_data(conn2, conn1, list_conn2_send_tables,
                                   list_conn1_recv_tables)
        if c2_flag:
            # print(f"...>> Data sending from {param2.name} to {param1.name} ..>> Done!!")
            print("...>> Data sending from {a} to {b} ..>> Done!!".format(a=param2.name, b=param1.name))


        # Close all connections & clean up
        # print(conn1, conn2)
        if conn1:
            conn1.close()
            conn1 = None

        if conn2:
            conn2.close()
            conn2 = None

        # print(conn1, conn2)
    
    def __send_data(self, send_conn, recv_conn, table_send, table_recv):
        # Helper function
        def conv_update_dml(tableName=None, rowDataStr=None):
            if tableName is None or rowDataStr is None:
                return None

            # updSqlStr = f"UPDATE {tableName} {rowDataStr}"
            updSqlStr = "UPDATE {a} {b}".format(a=tableName, b=rowDataStr)
            # print(updSqlStr)
            return updSqlStr
        #######################

        post_sqlStr_conn1 = self.__get_from_send_tables(send_conn, table_send)
        post_sqlStr_conn1_upd = []

        if post_sqlStr_conn1 is not None:
            for tname, rowDataStr in zip(table_recv, post_sqlStr_conn1):
                upd_str = conv_update_dml(tname, rowDataStr)
                if upd_str is not None:
                    post_sqlStr_conn1_upd.append(upd_str)

        # Update data to the remote table
        job_flag = self.__set_to_receive_tables(
            recv_conn, post_sqlStr_conn1_upd)
        # if job_flag:
        #     print("Data Transferred.")
        return job_flag

    # Check Network Status

    def __isNetworkActive(self, ip=None, port=None):
        flag = False
        if not ip or not port:
            return flag

        try:
            socket.setdefaulttimeout(20)
            socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect(
                (ip, port))
            flag = True
        except socket.error as err:
            print(str(e))
        except Exception as e:
            print(str(e))

        return flag

    def __checkDbConn(self, ip=None, user=None, passwd=None, port=1521, sid=None):
        """ Return database connection object"""
        # flag = False
        conObj = None
        if not ip or not user or not passwd or not port or not sid:
            return conObj

        connectionStr = "{user}/{passwd}@{ip}:{port}/{sid}".format(
            user=user, passwd=passwd, ip=ip, port=port, sid=sid)

        try:
            conObj = cx_Oracle.connect(connectionStr)
        except cx_Oracle.Error as e:
            print("Oracle database connection error: {}".format(str(e)))
        except Exception as e:
            print("General database connection error: {}".format(str(e)))

        return conObj

    def __get_from_send_tables(self, dbConn=None, sendTables=None):
        """ return insert and update string """
        if dbConn is None or sendTables is None or len(sendTables) <= 0:
            return None

        # helper function #
        def dml_str(colnames, data):
            counter = 0
            data_str = None
            if colnames is not None and data is not None:
                data_str = "SET "
                for col_header, rowval in zip(colnames, data):
                    # Discard these column values if found
                    if col_header not in ('AUSER', 'ADATE'):
                        # convert LDATE as Oracle Date String
                        if col_header in ('IDATE', 'LDATE'):
                            rowval = "TO_DATE('{a}', 'yyyy-mm-dd hh24:mi:ss')".format(a=rowval)
                        elif col_header in ('RINDEX'):
                            rowval = "%s" % rowval
                        else:
                            # Convert None rowval as decimal
                            if rowval is None or rowval == 'NULL':
                                rowval = 0.0
                            elif not rowval:
                                rowval = "%s" % rowval
                            else:
                                # format value with last three fraction number as string
                                rowval = "%.3f" % rowval
                        data_str += "{a}={b}".format(a=col_header, b=rowval)

                        # Add comma in between column values
                        if counter < len(colnames)-1:
                            data_str += ","

                        counter += 1
                        # print(data_str)
            return data_str

        sessStr = "ALTER SESSION SET NLS_DATE_FORMAT = 'yyyy-mm-dd hh24:mi:ss'"

        # container of all table data as string.
        data_str = None
        if dbConn:
            try:
                cursor = dbConn.cursor()
                cursor.execute(sessStr)
                data_str = []
                for table in sendTables:
                    # colnames = None
                    # qdata = None

                    sqlStr = "SELECT * FROM {a} WHERE RINDEX = (SELECT MAX(RINDEX) FROM {b})".format(a=table,b=table)
                    cursor.prepare(sqlStr)
                    cursor.execute(sqlStr)

                    result = cursor.fetchall()
                    if result:
                        qdata = []
                        colnames = []
                        # append column headers
                        colnames = [column_header[0]
                                    for column_header in cursor.description]
                        # append row values to data
                        for column_val in result:
                            for val in column_val:
                                qdata.append(val)
                        # Append sql string to list
                        sqlStr_t = dml_str(colnames, qdata)
                        data_str.append(sqlStr_t)
                # close the cursor
                cursor.close()

            except cx_Oracle.Error as e:
                print("Fetch data failed: {}".format(str(e)))
            except Exception as e:
                # print("Here I am")
                print("Send data General Error: {}".format(str(e)))

        return data_str

    def __set_to_receive_tables(self, dbConn=None, listOfUpdateSql=None):
        """
        Post data to the Destination Oracle Tables
        """
        flag = False
        if dbConn is None or listOfUpdateSql is None or len(listOfUpdateSql) <= 0:
            return flag

        if dbConn:
            try:
                cursor = dbConn.cursor()
                # loop through all tables
                for in_sql in listOfUpdateSql:
                    cursor.prepare(in_sql)
                    cursor.execute(in_sql)
                    # print(in_sql)
                cursor.close()
                dbConn.commit()
                flag = True
            except cx_Oracle.Error as e:
                print("Oracle Update Error: {}".format(str(e)))
            except Exception as e:
                print("General update Error: {}".format(str(e)))

        return flag


# configuration json parsing
def get_app_config(config_file=None):
    """
    USAGE:
    print(obj.app[0].active, obj.app[0].comm)
    """
    conf_obj = None
    if config_file is None:
        print("Valid confguration file required...")
        return conf_obj

    try:
        if os.path.exists(config_file):
            with open(config_file, 'r') as conf:
                app_conf = json.load(conf)
                json_str = json.dumps(app_conf)
                # parse json into an object with attributes corresponding to dict keys
                conf_obj = json.loads(
                    json_str, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    except SyntaxError as e:
        print("Parsing error: {}".format(str(e)))
    except Exception as e:
        print("File error: {}".format(str(e)))

    return conf_obj


def scheduler(runflag=False):
    # load config from json
    config_file = "/home/LinuxHistorian/Downloads/zxferv2/app.json"
    
    conf = get_app_config(config_file = config_file)

    if conf is None:
        print("Wrong file or path: {}".format(config_file))
        return False

    app_conf = conf.app

    # Automate it    
    while runflag:
        t = time.localtime()
        hr, min, sec = t.tm_hour, t.tm_min, t.tm_sec
        if hr in range(23) and min in range(60) and sec==2:
            print("Timestamp: {}".format(time.strftime('%m/%d/%Y %H:%M:%S', t)))
            # For all communication pair in the conf json.
            for comm_obj in app_conf:
                # call object
                app = dbDataTunneling(conf_obj=comm_obj)

            time.sleep(30)
            


def main():
    # obj = get_app_config()
    # # print(type(obj.app))
    # for cnf in obj.app:
    #     # print(cnf.active)
    #     # print(cnf.comm)
    #     # print("\n")
    #     for client in cnf.comm:
    #         print(client.name, client.post, client.get)
    run_flag = True
    try:        
        scheduler(run_flag)
    except KeyboardInterrupt:
        run_flag = False
        print("Stopping...")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)


if __name__ == "__main__":
    main()
