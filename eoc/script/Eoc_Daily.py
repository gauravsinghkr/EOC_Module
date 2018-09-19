# coding=utf-8
# !/usr/bin/env python

import datetime
import pandas as pd
import numpy as np
np.seterr(divide='ignore')
from xlsxwriter.utility import xl_rowcol_to_cell
import pandas.io.formats.excel
from functools import reduce
from SQLScript import SqlScript

pandas.io.formats.excel.header_style = None


class Daily(object):
    """
To create display placements
    """

    def __init__(self, config, sqlscript):

        """Accessing Files"""

        #super(Daily,self).__init__(self)
        self.config = config
        self.sqlscript = sqlscript
        self.logger = self.config.logger
        self.display_sales_first_table = None
        self.adsize_sales_second_table = None
        self.daily_sales_third_table = None
        self.placement_sales_data = None
        self.final_adsize = None
        self.final_day_wise = None

    def access_Data_KM_Sales_daily(self):

        """Accessing Columns by merging with summary"""
        display_sales_first_table = None
        try:
            if self.sqlscript.read_sql__display.empty:
                pass
            else:
                standard_sales_first_table = self.sqlscript.read_sql__display.merge(self.sqlscript.read_sql__display_mv, on="PLACEMENT#")
                display_exchange_first = standard_sales_first_table[
                    ["IO_ID", "PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE",
                     "NET_UNIT_COST","GROSS_UNIT_COST", "BOOKED_IMP#BOOKED_ENG", "DELIVERED_IMPRESSION",
                     "CLICKS", "CONVERSION"]]

                display_first_table = [display_exchange_first, self.config.cdb_io_exchange]

                display_first_table_io = reduce(lambda left, right: pd.merge(left, right, on='IO_ID'),
                                                display_first_table)


                mask_display_unit_au_nz_gb_not_null = (display_first_table_io["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (display_first_table_io["GROSS_UNIT_COST"]!=0)#.notnull())
                choices_display_unit_au_nz_gb_not_null = display_first_table_io["GROSS_UNIT_COST"] * display_first_table_io["Currency Exchange Rate"]

                mask_display_unit_au_nz_gb_is_null = (display_first_table_io["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (display_first_table_io["GROSS_UNIT_COST"] == 0)#.isnull())
                choices_display_unit_au_nz_is_null = display_first_table_io["NET_UNIT_COST"] * display_first_table_io["Currency Exchange Rate"]

                mask_display_unit_net_not_null = (~display_first_table_io["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (display_first_table_io["NET_UNIT_COST"]!=0)#.notnull())
                choices_display_unit_net_not_null = display_first_table_io["NET_UNIT_COST"] * display_first_table_io["Currency Exchange Rate"]

                mask_display_unit_net_is_null = (~display_first_table_io["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (display_first_table_io["NET_UNIT_COST"]==0)#.isnull())
                choices_display_unit_net_is_null = display_first_table_io["GROSS_UNIT_COST"] * display_first_table_io["Currency Exchange Rate"]

                display_first_table_io["UNIT_COST"] = np.select([mask_display_unit_au_nz_gb_not_null,
                                                                 mask_display_unit_au_nz_gb_is_null,
                                                                 mask_display_unit_net_not_null,
                                                                 mask_display_unit_net_is_null],
                                                                [choices_display_unit_au_nz_gb_not_null,
                                                                 choices_display_unit_au_nz_is_null,
                                                                 choices_display_unit_net_not_null,
                                                                 choices_display_unit_net_is_null],
                                                                default=0.00)

                display_sales_first_table = display_first_table_io[["PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE",
                                                                    "UNIT_COST", "BOOKED_IMP#BOOKED_ENG",
                                                                    "DELIVERED_IMPRESSION", "CLICKS", "CONVERSION"]]


        except (AttributeError, KeyError) as e:
            self.logger.error(str(e))
            pass

        adsize_sales_second_table = None
        try:
            if self.sqlscript.read_sql_adsize_mv.empty:
                pass
            else:
                standard_sales_second_table = self.sqlscript.read_sql__display.merge(self.sqlscript.read_sql_adsize_mv, on="PLACEMENT#")

                display_exchange_second = standard_sales_second_table[["IO_ID", "PLACEMENT#", "PLACEMENT_NAME",
                                                                       "COST_TYPE", "NET_UNIT_COST","GROSS_UNIT_COST", "BOOKED_IMP#BOOKED_ENG",
                                                                       "ADSIZE", "DELIVERED_IMPRESSION", "CLICKS",
                                                                       "CONVERSION"]]


                display_second_table = [display_exchange_second, self.config.cdb_io_exchange]

                display_second = reduce(lambda left, right: pd.merge(left, right, on='IO_ID'), display_second_table)

                mask_display_unit_au_nz_gb_not_null = (display_second["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (display_second["GROSS_UNIT_COST"]!=0)#.notnull())
                choices_display_unit_au_nz_gb_not_null = display_second["GROSS_UNIT_COST"] * display_second["Currency Exchange Rate"]

                mask_display_unit_au_nz_gb_is_null = (display_second["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (display_second["GROSS_UNIT_COST"]==0)#.isnull())
                choices_display_unit_au_nz_gb_is_null = display_second["NET_UNIT_COST"] * display_second["Currency Exchange Rate"]

                mask_display_unit_not_null = (~display_second["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (display_second["NET_UNIT_COST"]!=0)#.notnull())
                choices_display_unit_not_null = display_second["NET_UNIT_COST"] * display_second["Currency Exchange Rate"]

                mask_display_unit_is_null = (~display_second["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (display_second["NET_UNIT_COST"]==0)#.isnull())
                choices_display_unit_is_null = display_second["GROSS_UNIT_COST"] * display_second["Currency Exchange Rate"]

                display_second["UNIT_COST"] = np.select([mask_display_unit_au_nz_gb_not_null,
                                                         mask_display_unit_au_nz_gb_is_null,
                                                         mask_display_unit_not_null,
                                                         mask_display_unit_is_null],
                                                        [choices_display_unit_au_nz_gb_not_null,
                                                         choices_display_unit_au_nz_gb_is_null,
                                                         choices_display_unit_not_null,
                                                         choices_display_unit_is_null],default=0.00)


                adsize_sales_second_table = display_second[["PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE", "UNIT_COST",
                                                            "BOOKED_IMP#BOOKED_ENG", "ADSIZE", "DELIVERED_IMPRESSION",
                                                            "CLICKS", "CONVERSION"]]

        except (AttributeError, KeyError) as e:
            self.logger.error(str(e))
            pass

        daily_sales_third_table = None
        try:
            if self.sqlscript.read_sql_daily_mv.empty:
                pass
            else:
                standard_sales_third_table = self.sqlscript.read_sql__display.merge(self.sqlscript.read_sql_daily_mv, on="PLACEMENT#")

                display_io = standard_sales_third_table[["IO_ID", "PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE",
                                                         "NET_UNIT_COST","GROSS_UNIT_COST",
                                                         "BOOKED_IMP#BOOKED_ENG", "DAY", "DELIVERED_IMPRESSION",
                                                         "CLICKS", "CONVERSION"]]

                display_exchange = [display_io, self.config.cdb_io_exchange]

                display_info = reduce(lambda left, right: pd.merge(left, right, on='IO_ID'), display_exchange)

                mask_display_unit_aus_not_null = (display_info["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (display_info["GROSS_UNIT_COST"]!=0)#.notnull())
                choices_display_unit_aus_not_null= display_info["GROSS_UNIT_COST"] * display_info["Currency Exchange Rate"]

                mask_display_unit_aus_is_null = (display_info["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (display_info["GROSS_UNIT_COST"]==0)#.isnull())
                choices_display_unit_aus_is_null = display_info["NET_UNIT_COST"] * display_info["Currency Exchange Rate"]

                mask_display_unit_other_not_null = (~display_info["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (display_info["NET_UNIT_COST"]!=0)#.notnull())
                choices_display_unit_other_not_null = display_info["NET_UNIT_COST"] * display_info["Currency Exchange Rate"]

                mask_display_unit_other_is_null = (~display_info["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (display_info["NET_UNIT_COST"]==0)#.isnull())
                choices_display_unit_other_is_null = display_info["GROSS_UNIT_COST"] * display_info["Currency Exchange Rate"]


                display_info["UNIT_COST"] = np.select([mask_display_unit_aus_not_null,
                                                       mask_display_unit_aus_is_null,
                                                       mask_display_unit_other_not_null,
                                                       mask_display_unit_other_is_null],
                                                      [choices_display_unit_aus_not_null,
                                                       choices_display_unit_aus_is_null,
                                                       choices_display_unit_other_not_null,
                                                       choices_display_unit_other_is_null],default=0.00)

                daily_sales_third_table = display_info[["PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE",
                                                        "UNIT_COST", "BOOKED_IMP#BOOKED_ENG", "DAY",
                                                        "DELIVERED_IMPRESSION", "CLICKS", "CONVERSION"]]


        except (AttributeError, KeyError) as e:
            self.logger.error(str(e))
            pass

        self.display_sales_first_table = display_sales_first_table
        self.adsize_sales_second_table = adsize_sales_second_table
        self.daily_sales_third_table = daily_sales_third_table


    def KM_Sales_daily(self):
        """

       Joining Creative with placement Number
        """
        try:

            if self.sqlscript.read_sql__display.empty:
                pass
            else:

                mask1 = self.display_sales_first_table["COST_TYPE"].isin(['CPM'])
                mask4 = self.display_sales_first_table["COST_TYPE"].isin(['CPC'])

                self.display_sales_first_table["PLACEMENTNAME"] = self.display_sales_first_table[["PLACEMENT#", "PLACEMENT_NAME"]].apply(lambda x: ".".join(x), axis=1)

                choices_display_ctr = self.display_sales_first_table["CLICKS"] / self.display_sales_first_table["DELIVERED_IMPRESSION"]

                choices_display_conversion = self.display_sales_first_table["CONVERSION"] / \
                                             self.display_sales_first_table[
                                                 "DELIVERED_IMPRESSION"]

                choices_display_spend_cpm = self.display_sales_first_table["DELIVERED_IMPRESSION"] / 1000 * \
                                            self.display_sales_first_table[
                                                "UNIT_COST"]

                choices_display_spend_cpc = self.display_sales_first_table["CLICKS"] * self.display_sales_first_table["UNIT_COST"]

                self.display_sales_first_table["CTR"] = np.select([mask1, mask4],
                                                                  [choices_display_ctr, choices_display_ctr],
                                                                  default=0.00)

                self.display_sales_first_table["CTR"] = pd.to_numeric(self.display_sales_first_table.CTR,
                                                                      errors='coerce')

                self.display_sales_first_table["CONVERSIONRATE"] = np.select([mask1], [choices_display_conversion],
                                                                             default=0.00)

                self.display_sales_first_table["CONVERSIONRATE"] = pd.to_numeric(self.display_sales_first_table.CONVERSIONRATE,
                                                                                 errors='coerce')

                self.display_sales_first_table["SPEND"] = np.select([mask1, mask4],
                                                                    [choices_display_spend_cpm,
                                                                     choices_display_spend_cpc],
                                                                    default=0.00)

                self.display_sales_first_table["SPEND"] = pd.to_numeric(self.display_sales_first_table.SPEND,
                                                                        errors='coerce')

                self.display_sales_first_table["ECPA"] = self.display_sales_first_table["SPEND"] / self.display_sales_first_table["CONVERSION"]

                self.display_sales_first_table["ECPA"] = pd.to_numeric(self.display_sales_first_table.ECPA,
                                                                       errors='coerce')

            if self.sqlscript.read_sql_adsize_mv.empty:
                pass
            else:

                mask2 = self.adsize_sales_second_table["COST_TYPE"].isin(["CPM"])
                mask5 = self.adsize_sales_second_table["COST_TYPE"].isin(["CPC"])

                self.adsize_sales_second_table["PLACEMENTNAME"] = self.adsize_sales_second_table[
                    ["PLACEMENT#", "PLACEMENT_NAME"]].apply(
                    lambda x: ".".join(x), axis=1)

                choices_adsize_ctr = self.adsize_sales_second_table["CLICKS"] / self.adsize_sales_second_table[
                    "DELIVERED_IMPRESSION"]

                choices_adsize_conversion = self.adsize_sales_second_table[
                                                "CONVERSION"] / self.adsize_sales_second_table["DELIVERED_IMPRESSION"]

                choices_adsize_spend_cpm = self.adsize_sales_second_table["DELIVERED_IMPRESSION"] / 1000 * \
                                           self.adsize_sales_second_table[
                                               "UNIT_COST"]

                choices_adsize_spend_cpc = self.adsize_sales_second_table["CLICKS"] * self.adsize_sales_second_table["UNIT_COST"]

                self.adsize_sales_second_table["CTR"] = np.select([mask2, mask5], [choices_adsize_ctr,
                                                                                   choices_adsize_ctr], default=0.00)

                self.adsize_sales_second_table["CTR"] = pd.to_numeric(self.adsize_sales_second_table.CTR,
                                                                      errors='coerce')

                self.adsize_sales_second_table["CONVERSIONRATE"] = np.select([mask2], [choices_adsize_conversion],
                                                                             default=0.00)

                self.adsize_sales_second_table["CONVERSIONRATE"] = pd.to_numeric(self.adsize_sales_second_table.CONVERSIONRATE,
                                                                                 errors='coerce')

                self.adsize_sales_second_table["SPEND"] = np.select([mask2, mask5], [choices_adsize_spend_cpm,
                                                                                     choices_adsize_spend_cpc],
                                                                    default=0.00)

                self.adsize_sales_second_table["SPEND"] = pd.to_numeric(self.adsize_sales_second_table.SPEND,errors='coerce')

                self.adsize_sales_second_table["ECPA"] = self.adsize_sales_second_table["SPEND"] / self.adsize_sales_second_table["CONVERSION"]

                self.adsize_sales_second_table["ECPA"] = pd.to_numeric(self.adsize_sales_second_table.ECPA,
                                                                       errors='coerce')

            if self.sqlscript.read_sql_daily_mv.empty:
                pass
            else:
                mask3 = self.daily_sales_third_table["COST_TYPE"].isin(["CPM"])
                mask6 = self.daily_sales_third_table["COST_TYPE"].isin(["CPC"])

                self.daily_sales_third_table["PLACEMENTNAME"] = self.daily_sales_third_table[["PLACEMENT#", "PLACEMENT_NAME"]].apply(lambda x: ".".join(x), axis=1)


                choice_daily_ctr = self.daily_sales_third_table["CLICKS"] / self.daily_sales_third_table["DELIVERED_IMPRESSION"]

                choice_daily_spend_cpm = self.daily_sales_third_table["DELIVERED_IMPRESSION"] / 1000 * self.daily_sales_third_table["UNIT_COST"]

                choice_daily_spend_cpc = self.daily_sales_third_table["CLICKS"] * self.daily_sales_third_table["UNIT_COST"]

                choice_daily_cpa = (self.daily_sales_third_table["DELIVERED_IMPRESSION"] / 1000 *
                                    self.daily_sales_third_table[
                                        "UNIT_COST"]) / self.daily_sales_third_table["CONVERSION"]

                self.daily_sales_third_table["CTR"] = np.select([mask3, mask6], [choice_daily_ctr, choice_daily_ctr],
                                                                default=0.00)

                self.daily_sales_third_table["CTR"] = pd.to_numeric(self.daily_sales_third_table.CTR,
                                                                    errors='coerce')

                self.daily_sales_third_table["SPEND"] = np.select([mask3, mask6], [choice_daily_spend_cpm,
                                                                                   choice_daily_spend_cpc],default=0.00)

                self.daily_sales_third_table["SPEND"] = pd.to_numeric(self.daily_sales_third_table.SPEND,
                                                                      errors='coerce')

                self.daily_sales_third_table["ECPA"] = self.daily_sales_third_table["SPEND"] /self.daily_sales_third_table["CONVERSION"]

                self.daily_sales_third_table["ECPA"] = pd.to_numeric(self.daily_sales_third_table.ECPA,
                                                                     errors='coerce')


        except (AttributeError, KeyError) as e:
            self.logger.error(str(e))
            pass


    def rename_KM_Sales_daily(self):
        """Renaming The columns of Previous Functions"""
        try:
            if self.sqlscript.read_sql__display.empty:
                pass
            else:
                rename_display_sales_first_table = self.display_sales_first_table.rename(
                    columns={
                        "PLACEMENT#": "Placement#", "PLACEMENT_NAME": "Placement Name",
                        "COST_TYPE": "Cost Type", "UNIT_COST": "Unit Cost",
                        "BOOKED_IMP#BOOKED_ENG": "Booked Impressions", "DELIVERED_IMPRESSION": "Delivered Impressions"
                        , "CLICKS": "Clicks",
                        "CONVERSION": "Conversion"
                        , "PLACEMENTNAME": "Placement# Name", "CTR": "CTR"
                        , "CONVERSIONRATE": "Conversion Rate"
                        , "SPEND": "Spend", "ECPA": "eCPA"
                    }, inplace=True)
        except (AttributeError, KeyError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.sqlscript.read_sql_adsize_mv.empty:
                pass
            else:
                rename_adsize_sales_second_table = self.adsize_sales_second_table.rename(
                    columns={
                        "PLACEMENT#": "Placement#", "PLACEMENT_NAME": "Placement Name",
                        "COST_TYPE": "Cost Type", "UNIT_COST": "Unit Cost",
                        "BOOKED_IMP#BOOKED_ENG": "Booked", "ADSIZE": "Adsize"
                        , "DELIVERED_IMPRESSION": "Delivered Impressions", "CLICKS": "Clicks", "CONVERSION": "Conversion",
                        "PLACEMENTNAME": "Placement# Name"
                        , "CTR": "CTR", "CONVERSIONRATE": "Conversion Rate", "SPEND": "Spend", "ECPA": "eCPA"
                    }, inplace=True)
        except (AttributeError, KeyError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.sqlscript.read_sql_daily_mv.empty:
                pass
            else:
                rename_daily_sales_third_table = self.daily_sales_third_table.rename(
                    columns={
                        "PLACEMENT#": "Placement#", "PLACEMENT_NAME": "Placement Name",
                        "COST_TYPE": "Cost Type", "UNIT_COST": "Unit Cost", "BOOKED_IMP#BOOKED_ENG": "Booked",
                        "DAY": "Date", "DELIVERED_IMPRESSION": "Delivered Impressions", "CLICKS": "Clicks",
                        "CONVERSION": "Conversion", "PLACEMENTNAME": "Placement# Name",
                        "CTR": "CTR", "SPEND": "Spend", "ECPA": "eCPA"
                    }, inplace=True)

        except(AttributeError, KeyError) as e:
            self.logger.error(str(e))
            pass


    def accessing_nan_values(self):
        """Nan values handling"""
        try:
            if self.sqlscript.read_sql__display.empty:
                pass
            else:
                self.display_sales_first_table["CTR"] = self.display_sales_first_table["CTR"].replace([np.nan,np.inf], 0.00)
                self.display_sales_first_table["Conversion Rate"] = self.display_sales_first_table["Conversion Rate"].replace([np.nan,np.inf], 0.00)
                self.display_sales_first_table["Spend"] = self.display_sales_first_table["Spend"].replace([np.nan,np.inf], 0.00)
                self.display_sales_first_table["eCPA"] = self.display_sales_first_table["eCPA"].replace([np.nan,np.inf], 0.00)

        except (AttributeError, KeyError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.sqlscript.read_sql__display.empty:
                pass
            else:
                self.adsize_sales_second_table["CTR"] = self.adsize_sales_second_table["CTR"].replace([np.nan,np.inf], 0.00)
                self.adsize_sales_second_table["Conversion Rate"] = self.adsize_sales_second_table["Conversion Rate"].replace([np.nan,np.inf], 0.00)
                self.adsize_sales_second_table["Spend"] = self.adsize_sales_second_table["Spend"].replace([np.nan,np.inf], 0.00)
                self.adsize_sales_second_table["eCPA"] = self.adsize_sales_second_table["eCPA"].replace([np.nan,np.inf], 0.00)

        except (AttributeError, KeyError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.sqlscript.read_sql__display.empty:
                pass
            else:
                self.daily_sales_third_table["CTR"] = self.daily_sales_third_table["CTR"].replace([np.nan,np.inf], 0.00)
                self.daily_sales_third_table["Spend"] = self.daily_sales_third_table["Spend"].replace([np.nan,np.inf], 0.00)
                self.daily_sales_third_table["eCPA"] = self.daily_sales_third_table["eCPA"].replace([np.nan,np.inf], 0.00)

        except(AttributeError, KeyError) as e:
            self.logger.error(str(e))
            pass


    def accessing_main_column(self):
        """Accessing columns"""
        # debug = detailed info
        # info =confirmation that things accroding to the plan
        # warning = something unexpected
        # error = some function failed
        # critical = something failed application must close

        placement_sales_data = None
        final_adsize = None
        final_day_wise = None

        try:
            placement_sales_data = self.display_sales_first_table[["Placement# Name", "Unit Cost", "Booked Impressions",
                                                                   "Delivered Impressions", "Clicks", "CTR",
                                                                   "Conversion", "Spend", "eCPA"]]

            adsize_sales_data_new = self.adsize_sales_second_table.loc[:,
                                    ["Placement# Name", "Adsize", "Delivered Impressions", "Clicks",
                                     "CTR", "Conversion", "Conversion Rate", "Spend", "eCPA"]]

            final_adsize = adsize_sales_data_new[["Placement# Name", "Adsize", "Delivered Impressions", "Clicks", "CTR",
                                                  "Conversion", "Spend", "eCPA"]]


            daily_sales_data = self.daily_sales_third_table.loc[:,
                               ["Placement#", "Placement# Name", "Date", "Delivered Impressions",
                                "Clicks", "CTR", "Conversion", "eCPA", "Spend",
                                "Unit Cost"]]

            daily_sales_remove_zero = daily_sales_data[daily_sales_data['Delivered Impressions'] == 0]

            daily_sales_data = daily_sales_data.drop(daily_sales_remove_zero.index, axis=0)

            daily_sales_data["Date"] = pd.to_datetime(daily_sales_data["Date"])

            daily_sales_data['Date'] = pd.to_datetime(daily_sales_data['Date']).dt.date

            excel_start_date = datetime.date(1899, 12, 30)
            daily_sales_data['Date'] = daily_sales_data['Date'] - excel_start_date

            daily_sales_data.Date = daily_sales_data.Date.dt.days

            final_day_wise = daily_sales_data.loc[:, ["Placement# Name", "Date",
                                                      "Delivered Impressions", "Clicks", "CTR",
                                                      "Conversion", "Spend", "eCPA"]]


        except (AttributeError, KeyError) as e:
            self.logger.error(str(e))
            pass

        self.placement_sales_data = placement_sales_data
        self.final_adsize = final_adsize
        self.final_day_wise = final_day_wise


    def write_KM_Sales_summary(self):
        """writing Data"""

        unqiue_final_day_wise = 0
        try:
            unqiue_final_day_wise = self.final_day_wise['Placement# Name'].nunique()
        except KeyError as e:
            self.logger.error(str(e))
            pass

        try:
            info_client = self.config.client_info.to_excel(self.config.writer, sheet_name="Performance Details",
                                                           startcol=1, startrow=1, index=True, header=False)
            info_campaign = self.config.campaign_info.to_excel(self.config.writer, sheet_name="Performance Details",
                                                               startcol=1, startrow=2, index=True, header=False)
            info_ac_mgr = self.config.ac_mgr.to_excel(self.config.writer, sheet_name="Performance Details", startcol=4,
                                                      startrow=1, index=True, header=False)
            info_sales_rep = self.config.sales_rep.to_excel(self.config.writer, sheet_name="Performance Details",
                                                            startcol=4, startrow=2, index=True, header=False)
            info_campaign_date = self.config.sdate_edate_final.to_excel(self.config.writer,
                                                                        sheet_name="Performance Details", startcol=7,
                                                                        startrow=1, index=True, header=False)
            info_agency = self.config.agency_info.to_excel(self.config.writer, sheet_name="Performance Details",
                                                           startcol=1, startrow=3, index=True, header=False)
            info_currency = self.config.currency_info.to_excel(self.config.writer, sheet_name="Performance Details",
                                                               startcol=7, startrow=3, index=True, header=False)



        except (AttributeError, KeyError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.placement_sales_data.empty:
                pass
            else:
                writing_placement_data = self.placement_sales_data.to_excel(self.config.writer,
                                                                            sheet_name="Performance Details",
                                                                            startcol=1, startrow=8, index=False,
                                                                            header=True)
        except(AttributeError, KeyError) as e:
            self.logger.error(str(e))
            pass

        try:

            start_row_adsize = len(self.placement_sales_data) + 14

            if self.final_adsize.empty:
                pass
            else:
                for placement, placement_df in self.final_adsize.groupby('Placement# Name',sort=False, as_index=False):
                    writing_adsize_data = placement_df.to_excel(self.config.writer,
                                                                sheet_name="Performance Details",
                                                                startcol=1, startrow=start_row_adsize,
                                                                index=False,
                                                                header=False)


                    workbook = self.config.writer.book
                    worksheet = self.config.writer.sheets["Performance Details".format(self.config.ioid)]
                    start_row_adsize += len(placement_df) + 2
                    worksheet.write_string(start_row_adsize - 2, 1, 'Subtotal')
                    start_row_new = start_row_adsize - len(placement_df) - 1
                    format_num = workbook.add_format({"num_format": "#,##0"})
                    percent_fmt = workbook.add_format({"num_format": "0.00%", "align": "right"})
                    money_fmt = workbook.add_format({"num_format": "$#,###0.00", "align": "right"})
                    money_fmt_mxn = workbook.add_format({"num_format": '"MXN" #,###0.00', "align": "right"})
                    money_fmt_zar = workbook.add_format({"num_format": '"ZAR" #,###0.00', "align": "right"})
                    money_fmt_chf = workbook.add_format({"num_format": '"CHF" #,###0.00', "align": "right"})
                    money_fmt_inr = workbook.add_format({"num_format": u'₹#,###0.00', "align": "right"})
                    money_fmt_myr = workbook.add_format({"num_format": '"MYR" #,###0.00', "align": "right"})
                    money_fmt_thb = workbook.add_format({"num_format": '"THB" #,###0.00', "align": "right"})
                    money_fmt_eur = workbook.add_format({"num_format": u'€#,###0.00', "align": "right"})
                    money_fmt_gbp = workbook.add_format({"num_format": u'£#,###0.00', "align": "right"})

                    worksheet.write_formula(start_row_adsize - 2, 3,
                                            '=sum(D{}:D{})'.format(start_row_new, start_row_adsize - 2), format_num)

                    worksheet.write_formula(start_row_adsize - 2, 4,
                                            '=sum(E{}:E{})'.format(start_row_new, start_row_adsize - 2), format_num)

                    worksheet.write_formula(start_row_adsize - 2, 5,
                                            '=IFERROR(E{}/D{},0)'.format(start_row_adsize - 1, start_row_adsize - 1),
                                            percent_fmt)

                    worksheet.write_formula(start_row_adsize - 2, 6,
                                            '=sum(G{}:G{})'.format(start_row_new, start_row_adsize - 2), format_num)

                    worksheet.write_formula(start_row_adsize - 2, 7,
                                            '=sum(H{}:H{})'.format(start_row_new, start_row_adsize - 2), money_fmt)

                    worksheet.conditional_format(start_row_new - 1, 3, start_row_adsize - 2, 4,
                                                 {"type": "no_blanks", "format": format_num})

                    worksheet.conditional_format(start_row_new - 1, 5, start_row_adsize - 2, 5,
                                                 {"type": "no_blanks", "format": percent_fmt})

                    worksheet.conditional_format(start_row_new - 1, 6, start_row_adsize - 2, 6,
                                                 {"type": "no_blanks", "format": format_num})

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.conditional_format(start_row_new - 1, 7, start_row_adsize - 2, 8,
                                                     {"type": "no_blanks", "format": money_fmt_zar})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.conditional_format(start_row_new - 1, 7, start_row_adsize - 2, 8,
                                                     {"type": "no_blanks", "format": money_fmt_mxn})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.conditional_format(start_row_new - 1, 7, start_row_adsize - 2, 8,
                                                     {"type": "no_blanks", "format": money_fmt_thb})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.conditional_format(start_row_new - 1, 7, start_row_adsize - 2, 8,
                                                     {"type": "no_blanks", "format": money_fmt_eur})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.conditional_format(start_row_new - 1, 7, start_row_adsize - 2, 8,
                                                     {"type": "no_blanks", "format": money_fmt_gbp})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.conditional_format(start_row_new - 1, 7, start_row_adsize - 2, 8,
                                                     {"type": "no_blanks", "format": money_fmt_inr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.conditional_format(start_row_new - 1, 7, start_row_adsize - 2, 8,
                                                     {"type": "no_blanks", "format": money_fmt_myr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.conditional_format(start_row_new - 1, 7, start_row_adsize - 2, 8,
                                                     {"type": "no_blanks", "format": money_fmt_chf})

                    else:
                        worksheet.conditional_format(start_row_new - 1, 7, start_row_adsize - 2, 8,
                                                     {"type": "no_blanks", "format": money_fmt})

        except(AttributeError, KeyError) as e:
            self.logger.error(str(e))
            pass

        try:
            start_row_plc_day = len(self.placement_sales_data) + 13 + unqiue_final_day_wise * 2 + len(self.final_adsize) + 5

            if self.final_day_wise.empty is True:
                pass
            else:
                for placement_by_day, placement_df_by_day in self.final_day_wise.groupby('Placement# Name', sort =False,as_index=False):

                    writing_daily_data = placement_df_by_day.to_excel(self.config.writer, sheet_name="Performance Details",
                                                                      startcol=1, startrow=start_row_plc_day, index=False,
                                                                      header=False, merge_cells=False)
                    workbook = self.config.writer.book
                    worksheet = self.config.writer.sheets["Performance Details".format(self.config.ioid)]
                    start_row_plc_day += len(placement_df_by_day) + 2
                    worksheet.write_string(start_row_plc_day - 2, 1, 'Subtotal')
                    start_row_plc_day_new = start_row_plc_day - len(placement_df_by_day) - 1

                    format_num = workbook.add_format({"num_format": "#,##0"})
                    percent_fmt = workbook.add_format({"num_format": "0.00%", "align": "right"})

                    money_fmt = workbook.add_format({"num_format": "$#,###0.00", "align": "right"})
                    money_fmt_mxn = workbook.add_format({"num_format": '"MXN" #,###0.00', "align": "right"})
                    money_fmt_zar = workbook.add_format({"num_format": '"ZAR" #,###0.00', "align": "right"})
                    money_fmt_chf = workbook.add_format({"num_format": '"CHF" #,###0.00', "align": "right"})
                    money_fmt_inr = workbook.add_format({"num_format": u'₹#,###0.00', "align": "right"})
                    money_fmt_myr = workbook.add_format({"num_format": '"MYR" #,###0.00', "align": "right"})
                    money_fmt_thb = workbook.add_format({"num_format": '"THB" #,###0.00', "align": "right"})
                    money_fmt_eur = workbook.add_format({"num_format": u'€#,###0.00', "align": "right"})
                    money_fmt_gbp = workbook.add_format({"num_format": u'£#,###0.00', "align": "right"})

                    centre_date_format_wb = workbook.add_format({'align': 'center', 'num_format': 'YYYY-MM-DD'})
                    worksheet.conditional_format(start_row_plc_day_new - 1, 2, start_row_plc_day - 2, 2,
                                                 {"type": "no_blanks", "format": centre_date_format_wb})

                    worksheet.write_formula(start_row_plc_day - 2, 3, '=sum(D{}:D{})'.format(start_row_plc_day_new,
                                                                                             start_row_plc_day - 2),
                                            format_num)
                    worksheet.write_formula(start_row_plc_day - 2, 4, '=sum(E{}:E{})'.format(start_row_plc_day_new,
                                                                                             start_row_plc_day - 2),
                                            format_num)
                    worksheet.write_formula(start_row_plc_day - 2, 5, '=IFERROR(E{}/D{},0)'.format(start_row_plc_day - 1,
                                                                                                   start_row_plc_day - 1),
                                            percent_fmt)
                    worksheet.write_formula(start_row_plc_day - 2, 6, '=sum(G{}:G{})'.format(start_row_plc_day_new,
                                                                                             start_row_plc_day - 2),
                                            format_num)
                    worksheet.write_formula(start_row_plc_day - 2, 7, '=sum(H{}:H{})'.format(start_row_plc_day_new,
                                                                                             start_row_plc_day - 2),
                                            money_fmt)

                    worksheet.conditional_format(start_row_plc_day_new - 1, 3, start_row_plc_day - 2, 4,
                                                 {"type": "no_blanks", "format": format_num})

                    worksheet.conditional_format(start_row_plc_day_new - 1, 5, start_row_plc_day - 2, 5,
                                                 {"type": "no_blanks", "format": percent_fmt})

                    worksheet.conditional_format(start_row_plc_day_new - 1, 6, start_row_plc_day - 2, 6,
                                                 {"type": "no_blanks", "format": format_num})

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.conditional_format(start_row_plc_day_new - 1, 7, start_row_plc_day - 2, 8,
                                                     {"type": "no_blanks", "format": money_fmt_zar})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.conditional_format(start_row_plc_day_new - 1, 7, start_row_plc_day - 2, 8,
                                                     {"type": "no_blanks", "format": money_fmt_mxn})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.conditional_format(start_row_plc_day_new - 1, 7, start_row_plc_day - 2, 8,
                                                     {"type": "no_blanks", "format": money_fmt_thb})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.conditional_format(start_row_plc_day_new - 1, 7, start_row_plc_day - 2, 8,
                                                     {"type": "no_blanks", "format": money_fmt_eur})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.conditional_format(start_row_plc_day_new - 1, 7, start_row_plc_day - 2, 8,
                                                     {"type": "no_blanks", "format": money_fmt_gbp})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.conditional_format(start_row_plc_day_new - 1, 7, start_row_plc_day - 2, 8,
                                                     {"type": "no_blanks", "format": money_fmt_inr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.conditional_format(start_row_plc_day_new - 1, 7, start_row_plc_day - 2, 8,
                                                     {"type": "no_blanks", "format": money_fmt_myr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.conditional_format(start_row_plc_day_new - 1, 7, start_row_plc_day - 2, 8,
                                                     {"type": "no_blanks", "format": money_fmt_chf})
                    else:
                        worksheet.conditional_format(start_row_plc_day_new - 1, 7, start_row_plc_day - 2, 8,
                                                     {"type": "no_blanks", "format": money_fmt})

        except (AttributeError, KeyError) as e:
            self.logger.error(str(e))
            pass


    def formatting_daily(self):
        """
        Applying formatting on Display Sheet
        """

        try:

            workbook = self.config.writer.book
            worksheet = self.config.writer.sheets["Performance Details".format(self.config.ioid)]

            unqiue_final_day_wise = self.final_day_wise['Placement# Name'].nunique()
            format_grand = workbook.add_format({"bold": True, "bg_color": "#A5A5A5"})
            format_header = workbook.add_format({"bold": True, "bg_color": "#00B0F0"})
            format_header_center = workbook.add_format({"bold": True, "bg_color": "#00B0F0", "align": "center"})
            format_header_right = workbook.add_format({"bold": True, "bg_color": "#00B0F0", "align": "right"})

            format_colour = workbook.add_format({"bg_color": '#00B0F0'})
            format_campaign_info = workbook.add_format({"bold": True, "bg_color": '#00B0F0', "align": "left"})

            number_rows_placement = self.placement_sales_data.shape[0]
            number_cols_placement = self.placement_sales_data.shape[1]
            number_rows_adsize = self.final_adsize.shape[0]
            number_cols_adsize = self.final_adsize.shape[1]
            number_rows_daily = self.final_day_wise.shape[0]
            number_cols_daily = self.final_day_wise.shape[1]

            worksheet.hide_gridlines(2)
            worksheet.set_row(0, 6)
            worksheet.set_column("A:A", 2)
            worksheet.set_zoom(75)
            alignment_center = workbook.add_format({"align": "center"})

            alignment_right = workbook.add_format({"align": "right"})

            worksheet.conditional_format("A1:R5", {"type": "blanks", "format": format_campaign_info})
            worksheet.conditional_format("A1:R5", {"type": "no_blanks", "format": format_campaign_info})

            worksheet.insert_image("O7", "Exponential.png", {"url": "https://www.tribalfusion.com"})
            worksheet.insert_image("O2", "Client_Logo.png")

            format_header_left = workbook.add_format({"bold": True, "bg_color": '#00B0F0', "align": "left"})
            format_num = workbook.add_format({"num_format": "#,##0"})
            percent_fmt = workbook.add_format({"num_format": "0.00%", "align": "right"})

            money_fmt = workbook.add_format({"num_format": "$#,###0.00", "align": "right"})
            money_fmt_mxn = workbook.add_format({"num_format": '"MXN" #,###0.00', "align": "right"})
            money_fmt_zar = workbook.add_format({"num_format": '"ZAR" #,###0.00', "align": "right"})
            money_fmt_chf = workbook.add_format({"num_format": '"CHF" #,###0.00', "align": "right"})
            money_fmt_inr = workbook.add_format({"num_format": u'₹#,###0.00', "align": "right"})
            money_fmt_myr = workbook.add_format({"num_format": '"MYR" #,###0.00', "align": "right"})
            money_fmt_thb = workbook.add_format({"num_format": '"THB" #,###0.00', "align": "right"})
            money_fmt_eur = workbook.add_format({"num_format": u'€#,###0.00', "align": "right"})
            money_fmt_gbp = workbook.add_format({"num_format": u'£#,###0.00', "align": "right"})

            worksheet.write_string(7, 1, "Performance by Placement", format_header_left)
            worksheet.write_string(9 + number_rows_placement, 1, "Grand Total", format_grand)
            worksheet.conditional_format(7, 2, 7, number_cols_placement, {"type": "blanks", "format": format_colour})
            worksheet.conditional_format(7, 2, 7, number_cols_placement, {"type": "no_blanks", "format": format_colour})
            worksheet.conditional_format(8, 1, 8, 1, {"type": "no_blanks", "format": format_header_left})
            worksheet.conditional_format(8, 2, 8, 2, {"type": "no_blanks", "format": format_header})
            worksheet.conditional_format(8, 3, 8, 9, {"type": "no_blanks", "format": format_header})
            worksheet.write_string(2, 8, self.config.status)
            worksheet.write_string(2, 7, "Campaign Status")
            # worksheet.write_string (3, 1, "Agency Name")
            # worksheet.write_string (3, 7, "Currency")

            for col in range(3, 6):
                cell_location = xl_rowcol_to_cell(9 + number_rows_placement, col)
                start_range = xl_rowcol_to_cell(9, col)
                end_range = xl_rowcol_to_cell(9 + number_rows_placement - 1, col)
                formula = '=sum({:s}:{:s})'.format(start_range, end_range)
                worksheet.write_formula(cell_location, formula, format_num)
                start_plc_row = 9
                end_plc_row = 9 + number_rows_placement - 1
                worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                             {"type": "no_blanks", "format": format_num})
                start_range_format = 9 + number_rows_placement
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "no_blanks", "format": format_grand})
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "blanks", "format": format_grand})

            for col in range(6, 7):
                cell_location = xl_rowcol_to_cell(9 + number_rows_placement, col)
                formula = '=IFERROR(F{}/E{},0)'.format(9 + number_rows_placement + 1, 9 + number_rows_placement + 1)
                worksheet.write_formula(cell_location, formula, percent_fmt)
                start_plc_row = 9
                end_plc_row = 9 + number_rows_placement - 1
                worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                             {"type": "no_blanks", "format": percent_fmt})
                start_range_format = 9 + number_rows_placement
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "no_blanks", "format": format_grand})
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "blanks", "format": format_grand})

            for col in range(7, 8):
                cell_location = xl_rowcol_to_cell(9 + number_rows_placement, col)
                start_range = xl_rowcol_to_cell(9, col)
                end_range = xl_rowcol_to_cell(9 + number_rows_placement - 1, col)
                formula = '=sum({:s}:{:s})'.format(start_range, end_range)
                worksheet.write_formula(cell_location, formula, format_num)
                start_plc_row = 9
                end_plc_row = 9 + number_rows_placement - 1
                worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                             {"type": "no_blanks", "format": format_num})
                start_range_format = 9 + number_rows_placement
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "no_blanks", "format": format_grand})
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "blanks", "format": format_grand})

            for col in range(8, 9):
                cell_location = xl_rowcol_to_cell(9 + number_rows_placement, col)
                start_range = xl_rowcol_to_cell(9, col)
                end_range = xl_rowcol_to_cell(9 + number_rows_placement - 1, col)
                formula = '=sum({:s}:{:s})'.format(start_range, end_range)

                if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                    worksheet.write_formula(cell_location, formula, money_fmt_zar)

                elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                    worksheet.write_formula(cell_location, formula, money_fmt_mxn)

                elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                    worksheet.write_formula(cell_location, formula, money_fmt_thb)

                elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                    worksheet.write_formula(cell_location, formula, money_fmt_eur)

                elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                    worksheet.write_formula(cell_location, formula, money_fmt_gbp)

                elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                    worksheet.write_formula(cell_location, formula, money_fmt_inr)

                elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                    worksheet.write_formula(cell_location, formula, money_fmt_myr)

                elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                    worksheet.write_formula(cell_location, formula, money_fmt_chf)
                else:
                    worksheet.write_formula(cell_location, formula, money_fmt)

                start_plc_row = 9
                end_plc_row = 9 + number_rows_placement - 1

                if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_zar})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_mxn})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_thb})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_eur})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_gbp})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_inr})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_myr})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_chf})

                else:
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt})

                start_range_format = 9 + number_rows_placement
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "no_blanks", "format": format_grand})
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "blanks", "format": format_grand})

            for col in range(2, 3):
                start_plc_row = 9
                end_plc_row = 9 + number_rows_placement - 1

                if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_zar})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_mxn})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_thb})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_eur})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_gbp})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_inr})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_myr})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_chf})
                else:
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt})

                start_range = 9 + number_rows_placement
                worksheet.conditional_format(start_range, col, start_range, col, {"type": "blanks", "format": format_grand})
                worksheet.conditional_format(start_range, col, start_range, col,
                                             {"type": "no_blanks", "format": format_grand})

            for col in range(9, 10):
                start_plc_row = 9
                end_plc_row = 9 + number_rows_placement - 1

                if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_zar})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_mxn})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_thb})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_eur})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_gbp})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_inr})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_myr})

                elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt_chf})

                else:
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": money_fmt})

                start_range = 9 + number_rows_placement
                worksheet.conditional_format(start_range, col, start_range, col,
                                             {"type": "blanks", "format": format_grand})
                worksheet.conditional_format(start_range, col, start_range, col,
                                             {"type": "no_blanks", "format": format_grand})

            worksheet.write_string(12 + number_rows_placement, 1, "Performance by Ad Size", format_header_left)

            for col in range(2, number_cols_adsize + 1):
                worksheet.write_string(12 + number_rows_placement, col, "", format_colour)

            worksheet.write_string(13 + number_rows_placement, 1, "Placement # Name", format_header_left)
            worksheet.write_string(13 + number_rows_placement, 2, "Ad Size", format_header_center)
            worksheet.write_string(13 + number_rows_placement, 3, "Delivered Impressions", format_header_right)
            worksheet.write_string(13 + number_rows_placement, 4, "Clicks", format_header_right)
            worksheet.write_string(13 + number_rows_placement, 5, "CTR %", format_header_right)
            worksheet.write_string(13 + number_rows_placement, 6, "Conversions", format_header_right)
            worksheet.write_string(13 + number_rows_placement, 7, "Spend", format_header_right)
            worksheet.write_string(13 + number_rows_placement, 8, "eCPA", format_header_right)

            worksheet.write_string(13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2, 1,
                                   'Grand Total', format_grand)

            worksheet.write_formula(13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2, 3,
                                    '=SUMIF(B{}:B{},"Subtotal",D{}:D{})'.format(15 + number_rows_placement,
                                                                                13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2,
                                                                                15 + number_rows_placement,
                                                                                13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2),
                                    format_num)

            worksheet.write_formula(13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2, 4,
                                    '=SUMIF(B{}:B{},"Subtotal",E{}:E{})'.format(15 + number_rows_placement,
                                                                                13 + number_rows_placement
                                                                                + number_rows_adsize + unqiue_final_day_wise * 2,
                                                                                15 + number_rows_placement,
                                                                                13 + number_rows_placement
                                                                                + number_rows_adsize + unqiue_final_day_wise * 2),
                                    format_num)

            worksheet.write_formula(13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2, 5,
                                    '=IFERROR(E{}/D{},0)'.format(
                                        13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2 + 1,
                                        13 + number_rows_placement + number_rows_adsize
                                        + unqiue_final_day_wise * 2 + 1), percent_fmt)

            worksheet.write_formula(13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2, 6,
                                    '=SUMIF(B{}:B{},"Subtotal",G{}:G{})'.format(15 + number_rows_placement,
                                                                                13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2,
                                                                                15 + number_rows_placement,
                                                                                13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2),
                                    format_num)

            if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                worksheet.write_formula(13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2, 7,
                                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(15 + number_rows_placement,
                                                                                    13 + number_rows_placement
                                                                                    + number_rows_adsize + unqiue_final_day_wise * 2,
                                                                                    15 + number_rows_placement,
                                                                                    13 + number_rows_placement
                                                                                    + number_rows_adsize + unqiue_final_day_wise * 2),
                                        money_fmt_zar)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                worksheet.write_formula(13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2, 7,
                                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(15 + number_rows_placement,
                                                                                    13 + number_rows_placement
                                                                                    + number_rows_adsize + unqiue_final_day_wise * 2,
                                                                                    15 + number_rows_placement,
                                                                                    13 + number_rows_placement
                                                                                    + number_rows_adsize + unqiue_final_day_wise * 2),
                                        money_fmt_mxn)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                worksheet.write_formula(13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2, 7,
                                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(15 + number_rows_placement,
                                                                                    13 + number_rows_placement
                                                                                    + number_rows_adsize + unqiue_final_day_wise * 2,
                                                                                    15 + number_rows_placement,
                                                                                    13 + number_rows_placement
                                                                                    + number_rows_adsize + unqiue_final_day_wise * 2),
                                        money_fmt_thb)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                worksheet.write_formula(13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2, 7,
                                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(15 + number_rows_placement,
                                                                                    13 + number_rows_placement
                                                                                    + number_rows_adsize + unqiue_final_day_wise * 2,
                                                                                    15 + number_rows_placement,
                                                                                    13 + number_rows_placement
                                                                                    + number_rows_adsize + unqiue_final_day_wise * 2),
                                        money_fmt_eur)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                worksheet.write_formula(13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2, 7,
                                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(15 + number_rows_placement,
                                                                                    13 + number_rows_placement
                                                                                    + number_rows_adsize + unqiue_final_day_wise * 2,
                                                                                    15 + number_rows_placement,
                                                                                    13 + number_rows_placement
                                                                                    + number_rows_adsize + unqiue_final_day_wise * 2),
                                        money_fmt_gbp)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                worksheet.write_formula(13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2, 7,
                                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(15 + number_rows_placement,
                                                                                    13 + number_rows_placement
                                                                                    + number_rows_adsize + unqiue_final_day_wise * 2,
                                                                                    15 + number_rows_placement,
                                                                                    13 + number_rows_placement
                                                                                    + number_rows_adsize + unqiue_final_day_wise * 2),
                                        money_fmt_inr)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                worksheet.write_formula(13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2, 7,
                                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(15 + number_rows_placement,
                                                                                    13 + number_rows_placement
                                                                                    + number_rows_adsize + unqiue_final_day_wise * 2,
                                                                                    15 + number_rows_placement,
                                                                                    13 + number_rows_placement
                                                                                    + number_rows_adsize + unqiue_final_day_wise * 2),
                                        money_fmt_myr)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                worksheet.write_formula(13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2, 7,
                                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(15 + number_rows_placement,
                                                                                    13 + number_rows_placement
                                                                                    + number_rows_adsize + unqiue_final_day_wise * 2,
                                                                                    15 + number_rows_placement,
                                                                                    13 + number_rows_placement
                                                                                    + number_rows_adsize + unqiue_final_day_wise * 2),
                                        money_fmt_chf)

            else:
                worksheet.write_formula(13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2, 7,
                                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(15 + number_rows_placement,
                                                                                    13 + number_rows_placement
                                                                                    + number_rows_adsize + unqiue_final_day_wise * 2,
                                                                                    15 + number_rows_placement,
                                                                                    13 + number_rows_placement
                                                                                    + number_rows_adsize + unqiue_final_day_wise * 2),
                                        money_fmt)

            for col in range(2, number_cols_adsize + 1):
                worksheet.conditional_format(13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2,
                                             col,
                                             13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2,
                                             col,
                                             {"type": "blanks", "format": format_grand})
                worksheet.conditional_format(13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2,
                                             col,
                                             13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2,
                                             col,
                                             {"type": "no_blanks", "format": format_grand})

            grand_total_row = 13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2 + number_rows_daily + unqiue_final_day_wise * 2 + 4
            formula_range_grand = 14 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2 + 5
            writing_info_row = 13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2 + 3
            writing_header_row = 13 + number_rows_placement + number_rows_adsize + unqiue_final_day_wise * 2 + 4
            worksheet.write_string(writing_info_row, 1, "Performance - by Placement and Date", format_header_left)
            worksheet.write_string(writing_header_row, 1, "Placement # Name", format_header_left)
            worksheet.write_string(writing_header_row, 2, "Date", format_header_center)
            worksheet.write_string(writing_header_row, 3, "Delivered Impressions", format_header_right)
            worksheet.write_string(writing_header_row, 4, "Clicks", format_header_right)
            worksheet.write_string(writing_header_row, 5, "CTR %", format_header_right)
            worksheet.write_string(writing_header_row, 6, "Conversions", format_header_right)
            worksheet.write_string(writing_header_row, 7, "Spend", format_header_right)
            worksheet.write_string(writing_header_row, 8, "eCPA", format_header_right)
            worksheet.write_string(grand_total_row, 1, 'Grand Total', format_grand)

            for col in range(2, number_cols_daily + 1):
                worksheet.write_string(writing_info_row, col, "", format_colour)
                worksheet.conditional_format(grand_total_row, col, grand_total_row, col,
                                             {"type": "blanks", "format": format_grand})
                worksheet.conditional_format(grand_total_row, col, grand_total_row, col,
                                             {"type": "no_blanks", "format": format_grand})

            worksheet.write_formula(grand_total_row, 3, '=SUMIF(B{}:B{},"Subtotal",D{}:D{})'.format(formula_range_grand,
                                                                                                    grand_total_row,
                                                                                                    formula_range_grand,
                                                                                                    grand_total_row),
                                    format_num)

            worksheet.write_formula(grand_total_row, 4, '=SUMIF(B{}:B{},"Subtotal",E{}:E{})'.format(formula_range_grand,
                                                                                                    grand_total_row,
                                                                                                    formula_range_grand,
                                                                                                    grand_total_row),
                                    format_num)

            worksheet.write_formula(grand_total_row, 5,
                                    '=IFERROR(E{}/D{},0)'.format(grand_total_row + 1, grand_total_row + 1), percent_fmt)

            worksheet.write_formula(grand_total_row, 6, '=SUMIF(B{}:B{},"Subtotal",G{}:G{})'.format(formula_range_grand,
                                                                                                    grand_total_row,
                                                                                                    formula_range_grand,
                                                                                                    grand_total_row),
                                    format_num)

            if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                worksheet.write_formula(grand_total_row, 7,
                                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(formula_range_grand,
                                                                                    grand_total_row,
                                                                                    formula_range_grand,
                                                                                    grand_total_row), money_fmt_zar)
            elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                worksheet.write_formula(grand_total_row, 7,
                                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(formula_range_grand,
                                                                                    grand_total_row,
                                                                                    formula_range_grand,
                                                                                    grand_total_row), money_fmt_mxn)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                worksheet.write_formula(grand_total_row, 7,
                                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(formula_range_grand,
                                                                                    grand_total_row,
                                                                                    formula_range_grand,
                                                                                    grand_total_row), money_fmt_thb)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                worksheet.write_formula(grand_total_row, 7,
                                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(formula_range_grand,
                                                                                    grand_total_row,
                                                                                    formula_range_grand,
                                                                                    grand_total_row), money_fmt_eur)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                worksheet.write_formula(grand_total_row, 7,
                                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(formula_range_grand,
                                                                                    grand_total_row,
                                                                                    formula_range_grand,
                                                                                    grand_total_row), money_fmt_gbp)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                worksheet.write_formula(grand_total_row, 7,
                                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(formula_range_grand,
                                                                                    grand_total_row,
                                                                                    formula_range_grand,
                                                                                    grand_total_row), money_fmt_inr)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                worksheet.write_formula(grand_total_row, 7,
                                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(formula_range_grand,
                                                                                    grand_total_row,
                                                                                    formula_range_grand,
                                                                                    grand_total_row), money_fmt_myr)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                worksheet.write_formula(grand_total_row, 7,
                                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(formula_range_grand,
                                                                                    grand_total_row,
                                                                                    formula_range_grand,
                                                                                    grand_total_row), money_fmt_chf)
            else:
                worksheet.write_formula(grand_total_row, 7,
                                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(formula_range_grand,
                                                                                    grand_total_row,
                                                                                    formula_range_grand,
                                                                                    grand_total_row), money_fmt)

            alignment_left = workbook.add_format({"align": "left"})
            worksheet.set_column(1, 1, 45)
            worksheet.set_column(2, 2, 13, alignment_center)
            worksheet.set_column(3, 4, 20, alignment_right)
            worksheet.set_column(5, 6, 14, alignment_right)
            worksheet.set_column(7, 7, 21, alignment_right)
            worksheet.set_column(8, 9, 11, alignment_right)
            worksheet.set_column(10, 17, 15, alignment_right)
            worksheet.set_row(1, None, alignment_left)
            worksheet.set_row(2, None, alignment_left)
            worksheet.set_row(3, None, alignment_left)

        except (AttributeError, KeyError) as e:
            self.logger.error(str(e))
            pass


    def main(self):
        """Adding Main Function"""
        self.config.common_columns_summary()
        self.config.logger.info("Start Creating Performance Details Sheet for IO - {} ".format(self.config.ioid) + " at "+ str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        if self.sqlscript.read_sql__display.empty:
            self.logger.info("No live display placements for IO - {}".format(self.config.ioid) + " at "+ str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
            pass
        else:
            self.logger.info("Live display placements found for IO - {}".format(self.config.ioid)+ " at "+ str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
            self.access_Data_KM_Sales_daily()
            self.KM_Sales_daily()
            self.rename_KM_Sales_daily()
            self.accessing_nan_values()
            self.accessing_main_column()
            self.write_KM_Sales_summary()
            self.formatting_daily()
            self.logger.info("Performance Details Sheet Created for IO {}".format(self.config.ioid) + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))


if __name__ == "__main__":
    pass

# enable it when running for individual file
# c = config.Config('test', 606087,'2018-01-02','2018-02-02')
# o = Daily( c )
# o.main()
# c.saveAndCloseWriter()
