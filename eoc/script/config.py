# coding=utf-8
# !/usr/bin/env python
"""
Created by:Dharmendra
Date:2018-03-23
"""
import datetime
import pandas as pd
import cx_Oracle
from BiProper import Properties
from SQLScript import SqlScript
import logging


class Config(Properties):
    """
    This class is configuration for all classes
    """

    def __init__(self, end_date, ioid, start_date):

        super(Config,self).__init__()
        self.sqlscript = SqlScript
        self.client_info = None
        self.campaign_info = None

        self.ac_mgr = None
        self.sales_rep = None
        self.sdate_edate_final = None
        self.status = None
        self.logger = None
        self.cdb_value = None
        self.agency_info = None
        self.cdb_value_currency = None
        self.cdb_io_exchange = None
        self.currency_info = None
        self.io_discount = None

        self.start_date = start_date
        self.ioid = ioid
        self.end_date = end_date


        self.LoggFile()
        self.logger.info('Trying to connect with TFR for io: {}'.format(self.ioid))
        self.writer = pd.ExcelWriter(self.save_path + '{}.xlsx'.format(self.ioid) , engine="xlsxwriter", datetime_format="YYYY-MM-DD")

    def saveAndCloseWriter(self):
        """
        To finally Save and close file
        :return: Nothing
        """
        self.writer.save()
        self.writer.close()

    def common_columns_summary(self):
        """
        reading data from csv file for ioid
        read_common_columns, data_common_columns
        """

        sql_client_info = "SELECT DISTINCT CLIENT_DESC from TFR_REP.EOC_SUMMARY_VIEW WHERE IO_ID = {}".format(self.ioid)
        sql_campaign_info = "SELECT DISTINCT IO_DESC FROM TFR_REP.EOC_SUMMARY_VIEW WHERE IO_ID = {}".format(self.ioid)
        sql_acct_mgr = "SELECT DISTINCT ACCOUNT_MGR FROM TFR_REP.EOC_SUMMARY_VIEW WHERE IO_ID = {}".format(self.ioid)
        sql_sales_rep = "SELECT DISTINCT SALES_REP FROM TFR_REP.EOC_SUMMARY_VIEW WHERE IO_ID = {}".format(self.ioid)
        sql_end_date = "SELECT TO_CHAR(MAX(EDATE),'YYYY-MM-DD') as EDATENEW from TFR_REP.EOC_SUMMARY_VIEW WHERE IO_ID = {}".format(self.ioid)

        read_cdb = pd.read_csv(self.exchange)
        read_discount = pd.read_csv(self.discount)

        discount_value = read_discount.loc[read_discount['IO id'] == self.ioid]

        discount_value_rebate = discount_value.loc[:, ['IO id', 'Discount']]

        discount_value_rebate.rename(columns = {"IO id":"IO_ID"},inplace=True)

        io_discount = discount_value_rebate.loc[:,["IO_ID","Discount"]]

        cdb_value = read_cdb.loc[read_cdb['IO Id'] == self.ioid]
        cdb_value_agency = cdb_value.loc[:, ['Agency Name']]
        agency_info = cdb_value_agency.set_index('Agency Name').reset_index().transpose()

        cdb_value_currency = cdb_value.loc[:, ['Currency Type']]
        cdb_value_currency.rename(columns={"Currency Type": "Currency"}, inplace=True)
        currency_info = cdb_value_currency.set_index('Currency').reset_index().transpose()

        cdb_io_exchange_currency = cdb_value.loc[:, ['IO Id', 'IO Exchange Rate', 'Currency Type']]

        cdb_io_exchange_currency.rename(columns={"IO Id": "IO_ID", "IO Exchange Rate": "Currency Exchange Rate"},
                                        inplace=True)
        cdb_io_exchange = cdb_io_exchange_currency.loc[:, ["IO_ID", "Currency Exchange Rate", 'Currency Type']]

        read_sql_client_info = pd.read_sql(sql_client_info, cx_Oracle.connect(self.login))
        read_last_row_client_info = read_sql_client_info.iloc[-1:]
        read_last_row_client_info.rename(columns={"CLIENT_DESC": "Client Name"}, inplace=True)
        client_info = read_last_row_client_info.set_index('Client Name').reset_index().transpose()

        read_sql_io_info = pd.read_sql(sql_campaign_info, cx_Oracle.connect(self.login))
        read_last_row_io_info = read_sql_io_info.iloc[-1:]
        read_last_row_io_info.rename(columns={"IO_DESC": "Campaign Name"}, inplace=True)
        campaign_info = read_last_row_io_info.set_index('Campaign Name').reset_index().transpose()

        read_sql_acct_mgr = pd.read_sql(sql_acct_mgr, cx_Oracle.connect(self.login))
        read_last_row_sql_acct_mgr = read_sql_acct_mgr.iloc[-1:]
        read_last_row_sql_acct_mgr.rename(columns={"ACCOUNT_MGR": "Expo Account Manager"}, inplace=True)
        ac_mgr = read_last_row_sql_acct_mgr.set_index('Expo Account Manager').reset_index().transpose()

        read_sql_sales_rep = pd.read_sql(sql_sales_rep, cx_Oracle.connect(self.login))
        read_last_row_sales_rep = read_sql_sales_rep.iloc[-1:]
        read_last_row_sales_rep.rename(columns={"SALES_REP": "Expo Sales Contact"}, inplace=True)
        sales_rep = read_last_row_sales_rep.set_index('Expo Sales Contact').reset_index().transpose()


        read_new_start_date = pd.DataFrame({"Start_Date": [self.start_date]})


        read_new_end_date = pd.DataFrame({'End_Date': [self.end_date]})

        read_sql_end_date = pd.read_sql(sql_end_date,cx_Oracle.connect(self.login))
        read_last_row_end_date = read_sql_end_date.iloc[-1:]
        read_last_row_end_date.rename(columns={"EDATENEW": "End_Date_New"}, inplace=True)
        final_end_date = read_last_row_end_date.iloc[0, 0]

        sdate_edate = pd.concat([read_new_start_date, read_new_end_date], axis=1)
        try:
            sdate_edate["Campaign Report date"] = sdate_edate[["Start_Date", "End_Date"]].apply(lambda x: " to ".join(x), axis=1)
        except TypeError as e:
            self.logger.error(str(e))
            pass

        sdate_edate_new = sdate_edate.iloc[-1:, -1]
        sdate_edate_new = sdate_edate_new.to_frame()
        sdate_edate_final = None
        try:
            sdate_edate_final = sdate_edate_new.set_index('Campaign Report date').reset_index().transpose()
        except KeyError as e:
            self.logger.error(str(e))
            pass

        u_date = datetime.date.today()-datetime.timedelta(1)
        new_date = u_date.strftime('%Y-%m-%d')

        if final_end_date > new_date:
            status = "Live"

        else:
            status = "Completed"

        self.client_info = client_info
        self.campaign_info = campaign_info
        self.ac_mgr = ac_mgr
        self.sales_rep = sales_rep
        self.sdate_edate_final = sdate_edate_final
        self.status = status
        self.cdb_value = cdb_value
        self.agency_info = agency_info
        self.currency_info = currency_info
        self.cdb_io_exchange = cdb_io_exchange
        self.cdb_value_currency = cdb_value_currency
        self.io_discount = io_discount

    def LoggFile(self):
        # create logger with 'spam_application'
        """
        logger for console and output
        """
        logger = logging.getLogger('EOCApp')
        logger.setLevel(logging.DEBUG)

        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # create file handler which logs even debug messages
        fh = logging.FileHandler(self.log +"logfile({}).log".format(self.ioid))
        fh.setLevel(logging.ERROR)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        self.logger = logger
