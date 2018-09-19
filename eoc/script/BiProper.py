# !/usr/bin/python
# coding=utf-8

"""Configuration of absolute paths"""
import configparser


class Properties(object):
    """
    read bi.ini file
    """
    def __init__(self):
        """
        Path
        :return:
        """
        config_file = configparser.ConfigParser()
        config_file.read('bi.ini')
        self.exchange = config_file['BI-EOC-Properties']['Exchangepath']
        self.discount = config_file['BI-EOC-Properties']['Discountpath']
        self.login = config_file['BI-EOC-Properties']['TFR_DB_login']
        self.log = config_file['BI-EOC-Properties']['log_path']
        self.sql = config_file['BI-EOC-Properties']['sql_path']
        self.save_path = config_file['BI-EOC-Properties']['save_path']

if __name__ == "__main__":
    obj_read = Properties()
    #obj_read.main()




