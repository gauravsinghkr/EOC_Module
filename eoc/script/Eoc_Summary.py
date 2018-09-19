# coding=utf-8
# !/usr/bin/env python

from __future__ import print_function

import datetime
import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell
import pandas.io.formats.excel

pandas.io.formats.excel.header_style = None
from functools import reduce
from SQLScript import SqlScript


class Summary(object):
    """This class in for creating summary sheet"""

    def __init__(self, config, sqlscript):
        """Config"""

        #super(Summary,self).__init__(self)
        self.config = config
        self.sqlscript = sqlscript
        self.logger = self.config.logger
        self.displayfirsttable = None
        self.vdx_access_table = None
        self.preroll_access_table = None

    def access_display_summary(self):
        """
        Display Placement Summary
        """
        if self.sqlscript.read_sql__display.empty:
            pass
        else:
            display_first_exchange = [self.sqlscript.read_sql__display, self.sqlscript.read_sql__display_placement]
            display_table_info = reduce(lambda left, right: pd.merge(left, right, on='PLACEMENT#'),
                                        display_first_exchange)

            display_table = display_table_info[["IO_ID", "PLACEMENT#", "START_DATE", "END_DATE", "PLACEMENT_NAME",
                                                "COST_TYPE", "NET_UNIT_COST", "NET_PLANNED_COST", "GROSS_UNIT_COST",
                                                "GROSS_PLANNED_COST",
                                                "BOOKED_IMP#BOOKED_ENG",
                                                "DELIVERED_IMPRESSION", "CLICKS"]]

            mask_display_imp = display_table["COST_TYPE"].isin(['CPM'])
            mask_display_click = display_table["COST_TYPE"].isin(['CPC'])

            choice_display_imp = display_table["DELIVERED_IMPRESSION"]
            choice_display_click = display_table["CLICKS"]

            display_table["Delivered_Impressions"] = np.select([mask_display_imp, mask_display_click],
                                                               [choice_display_imp, choice_display_click])

            display_merge = [display_table, self.config.cdb_io_exchange]

            display_table_info = reduce(lambda left, right: pd.merge(left, right, on='IO_ID'), display_merge)

            mask_display_unit_au_nz_gb_gross = (display_table_info["Currency Type"].isin(['AUD','NZD', 'GBP'])) & (display_table_info["GROSS_UNIT_COST"]!=0)#.notnull())
            choices_display_unit_au_nz_gb_gross = display_table_info["GROSS_UNIT_COST"] * display_table_info["Currency Exchange Rate"]

            mask_display_unit_au_nz_gb_net = (display_table_info["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (display_table_info["GROSS_UNIT_COST"]==0)#.isnull())
            choices_display_unit_au_nz_gb_net = display_table_info["NET_UNIT_COST"] * display_table_info["Currency Exchange Rate"]

            mask_display_unit_net = (~display_table_info["Currency Type"].isin(['AUD','NZD', 'GBP'])) & (display_table_info["NET_UNIT_COST"]!=0)#.notnull())
            choices_display_unit_net = display_table_info["NET_UNIT_COST"] * display_table_info["Currency Exchange Rate"]

            mask_display_unit_gross = (~display_table_info["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (display_table_info["NET_UNIT_COST"]==0)#.isnull())
            choices_display_unit_gross = display_table_info["GROSS_UNIT_COST"] * display_table_info["Currency Exchange Rate"]

            display_table_info["UNIT_COST"] = np.select([mask_display_unit_au_nz_gb_gross,
                                                         mask_display_unit_au_nz_gb_net,
                                                         mask_display_unit_net,
                                                         mask_display_unit_gross],
                                                        [choices_display_unit_au_nz_gb_gross,
                                                         choices_display_unit_au_nz_gb_net,
                                                         choices_display_unit_net,
                                                         choices_display_unit_gross],
                                                        default=0.00)

            mask_gross_budget_au_nz_gb = (display_table_info["Currency Type"].isin(['AUD','NZD', 'GBP'])) & (display_table_info["GROSS_PLANNED_COST"]!=0)#.notnull())
            choice_gross_cost_au_nz_gb = display_table_info["GROSS_PLANNED_COST"] * display_table_info["Currency Exchange Rate"]

            mask_net_budget_au_nz_gb = (display_table_info["Currency Type"].isin(['AUD','NZD', 'GBP'])) & (display_table_info["GROSS_PLANNED_COST"]==0)#.isnull())
            choice_net_cost_au_nz_gb = display_table_info["NET_PLANNED_COST"] * display_table_info["Currency Exchange Rate"]

            mask_display_budget_net = ~display_table_info["Currency Type"].isin(['AUD', 'NZD', 'GBP']) & (display_table_info["NET_PLANNED_COST"]!=0)#.notnull())
            choice_net_budget = display_table_info["NET_PLANNED_COST"] * display_table_info["Currency Exchange Rate"]

            mask_display_net = ~display_table_info["Currency Type"].isin(['AUD', 'NZD', 'GBP']) & (display_table_info["NET_PLANNED_COST"]==0)#.isnull())
            choice_display_net = display_table_info["GROSS_PLANNED_COST"] * display_table_info["Currency Exchange Rate"]

            display_table_info["PLANNED_COST"] = np.select([mask_gross_budget_au_nz_gb,
                                                            mask_net_budget_au_nz_gb,
                                                            mask_display_budget_net,
                                                            mask_display_net],
                                                           [choice_gross_cost_au_nz_gb,
                                                            choice_net_cost_au_nz_gb,
                                                            choice_net_budget,
                                                            choice_display_net],default=0.00)


            displayfirsttable = display_table_info[["PLACEMENT#", "START_DATE", "END_DATE", "PLACEMENT_NAME",
                                                    "COST_TYPE", "UNIT_COST", "PLANNED_COST",
                                                    "BOOKED_IMP#BOOKED_ENG", "Delivered_Impressions"]]


            self.displayfirsttable = displayfirsttable


    def access_vdx_summary(self):
        """VDX Placements Summary"""

        if self.sqlscript.read_sql__v_d_x.empty:
            pass
        else:
            vdx_merge_data = [self.sqlscript.read_sql__v_d_x, self.sqlscript.read_sql__v_d_x_placement]

            vdx_second_summary = reduce(lambda left, right: pd.merge(left, right, on='PLACEMENT#'),vdx_merge_data)

            conditionseng = [(vdx_second_summary.loc[:, ['COST_TYPE']] == 'CPE'),
                             (vdx_second_summary.loc[:, ['COST_TYPE']] == 'CPE+')]

            choiceseng = [vdx_second_summary.loc[:, ["ENGAGEMENTS"]],vdx_second_summary.loc[:, ["DPEENGAGEMENTS"]]]

            vdx_second_summary["Delivered_Engagements"] = np.select(conditionseng, choiceseng)

            conditionsimp = [(vdx_second_summary.loc[:, ['COST_TYPE']] == 'CPCV'),
                             (vdx_second_summary.loc[:, ['COST_TYPE']] == 'CPM')]


            choiceimp = [vdx_second_summary.loc[:, ["COMPLETIONS"]],
                         vdx_second_summary.loc[:, ["IMPRESSIONS"]]]

            vdx_second_summary["Delivered_Impressions"] = np.select(conditionsimp, choiceimp)

            vdx_exchange_table = [vdx_second_summary, self.config.cdb_io_exchange]
            vdx_table = reduce(lambda left, right: pd.merge(left, right, on='IO_ID'), vdx_exchange_table)


            mask_vdx_unit_au_nz_gb_not_null = (vdx_table["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (vdx_table["GROSS_UNIT_COST"]!=0) #.notnull())
            choices_vdx_unit_au_nz_gb_not_null = vdx_table["GROSS_UNIT_COST"] * vdx_table["Currency Exchange Rate"]

            mask_vdx_unit_au_nz_gb_is_null = (vdx_table["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (vdx_table["GROSS_UNIT_COST"]==0)#.isnull())
            choices_vdx_unit_au_is_null = vdx_table["NET_UNIT_COST"] * vdx_table["Currency Exchange Rate"]

            mask_vdx_unit_net_not_null = (~vdx_table["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (vdx_table["NET_UNIT_COST"]!=0) #.notnull())
            choices_vdx_unit_net_not_null = vdx_table["NET_UNIT_COST"] * vdx_table["Currency Exchange Rate"]

            mask_vdx_unit_is_null = (~vdx_table["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (vdx_table["NET_UNIT_COST"]==0) #.isnull())
            choices_vdx_unit_is_null = vdx_table["GROSS_UNIT_COST"] * vdx_table["Currency Exchange Rate"]

            vdx_table['UNIT_COST'] = np.select([mask_vdx_unit_au_nz_gb_not_null,
                                                mask_vdx_unit_au_nz_gb_is_null,
                                                mask_vdx_unit_net_not_null,
                                                mask_vdx_unit_is_null],
                                               [choices_vdx_unit_au_nz_gb_not_null,
                                                choices_vdx_unit_au_is_null,
                                                choices_vdx_unit_net_not_null,
                                                choices_vdx_unit_is_null],default=0.00)

            mask_vdx_cost_au_nz_gb_not_null = (vdx_table["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (vdx_table["GROSS_PLANNED_COST"]!=0) #.notnull())
            choice_vdx_cost_au_nz_gb_not_null = vdx_table["GROSS_PLANNED_COST"] * vdx_table["Currency Exchange Rate"]

            mask_vdx_cost_au_nz_gb_is_null = (vdx_table["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (vdx_table["GROSS_PLANNED_COST"]==0) #.isnull())
            choice_vdx_cost_au_is_null = vdx_table["NET_PLANNED_COST"] * vdx_table["Currency Exchange Rate"]

            mask_vdx_cost_net_not_null = (~vdx_table["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (vdx_table["NET_PLANNED_COST"]!=0) #.notnull())
            choices_vdx_cost_net_not_null = vdx_table["NET_PLANNED_COST"] * vdx_table["Currency Exchange Rate"]

            mask_vdx_cost_is_null = (~vdx_table["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (vdx_table["NET_PLANNED_COST"]==0)  #.isnull())
            choices_vdx_cost_net_is_null = vdx_table["GROSS_PLANNED_COST"] * vdx_table["Currency Exchange Rate"]

            vdx_table['PLANNED_COST'] = np.select([mask_vdx_cost_au_nz_gb_not_null,
                                                   mask_vdx_cost_au_nz_gb_is_null ,
                                                   mask_vdx_cost_net_not_null ,
                                                   mask_vdx_cost_is_null],
                                                  [choice_vdx_cost_au_nz_gb_not_null,
                                                   choice_vdx_cost_au_is_null,
                                                   choices_vdx_cost_net_not_null,choices_vdx_cost_net_is_null],default=0.00)

            vdx_access_table = vdx_table[
                ["PLACEMENT#", "START_DATE", "END_DATE", "PLACEMENT_NAME", "COST_TYPE",
                 "UNIT_COST", "PLANNED_COST", "BOOKED_IMP#BOOKED_ENG",
                 "Delivered_Engagements", "Delivered_Impressions"]]

            self.vdx_access_table = vdx_access_table

    def access_preroll_summary(self):
        """Preroll Placements Summary"""

        if self.sqlscript.read_sql_preroll.empty:
            pass
        else:
            preroll_third_summary = self.sqlscript.read_sql_preroll.merge(self.sqlscript.read_sql_preroll_placement, on="PLACEMENT#")

            preroll_table = preroll_third_summary[
                ["IO_ID", "PLACEMENT#", "START_DATE", "END_DATE", "PLACEMENT_NAME",
                 "COST_TYPE", "NET_UNIT_COST", "NET_PLANNED_COST", "GROSS_UNIT_COST", "GROSS_PLANNED_COST",
                 "BOOKED_IMP#BOOKED_ENG",
                 "IMPRESSION", "COMPLETIONS"]]

            preroll_exchange_table = [preroll_table, self.config.cdb_io_exchange]

            preroll_final_table = reduce(lambda left, right: pd.merge(left, right, on='IO_ID'),
                                         preroll_exchange_table)

            mask_preroll_unit_au_nz_gb_not_null = (preroll_final_table["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (preroll_final_table["GROSS_UNIT_COST"]!=0)#.notnull())
            choices_preroll_unit_au_nz_gb_not_null = preroll_final_table["GROSS_UNIT_COST"] * preroll_final_table["Currency Exchange Rate"]

            mask_preroll_unit_au_nz_gb_is_null = (preroll_final_table["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (preroll_final_table["GROSS_UNIT_COST"]==0)#.isnull())
            choices_preroll_unit_au_nz_gb_is_null = preroll_final_table["NET_UNIT_COST"] * preroll_final_table["Currency Exchange Rate"]

            mask_preroll_unit_net_not_null = (~preroll_final_table["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (preroll_final_table["NET_UNIT_COST"]!=0)#.notnull())
            choices_preroll_unit_net_not_null = preroll_final_table["NET_UNIT_COST"] * preroll_final_table["Currency Exchange Rate"]

            mask_preroll_unit_net_is_null = (~preroll_final_table["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (preroll_final_table["NET_UNIT_COST"]==0)#.isnull())
            choices_preroll_unit_net_is_null = preroll_final_table["GROSS_UNIT_COST"] * preroll_final_table["Currency Exchange Rate"]

            preroll_final_table["UNIT_COST"] = np.select([mask_preroll_unit_au_nz_gb_not_null,
                                                          mask_preroll_unit_au_nz_gb_is_null,
                                                          mask_preroll_unit_net_not_null,
                                                          mask_preroll_unit_net_is_null],
                                                         [choices_preroll_unit_au_nz_gb_not_null,
                                                          choices_preroll_unit_au_nz_gb_is_null,
                                                          choices_preroll_unit_net_not_null,
                                                          choices_preroll_unit_net_is_null],
                                                         default=0.00)

            mask_preroll_cost_au_nz_gb_not_null = (preroll_final_table["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (preroll_final_table["GROSS_PLANNED_COST"]!=0)#.notnull())
            choice_preroll_cost_au_nz_gb_not_null = preroll_final_table["GROSS_PLANNED_COST"] * preroll_final_table["Currency Exchange Rate"]

            mask_preroll_cost_au_nz_gb_is_null = (preroll_final_table["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (preroll_final_table["GROSS_PLANNED_COST"]==0)#.isnull())
            choice_preroll_cost_au_nz_gb_is_null =  preroll_final_table["NET_PLANNED_COST"] * preroll_final_table["Currency Exchange Rate"]

            mask_preroll_cost_net_not_null = (~preroll_final_table["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (preroll_final_table["NET_PLANNED_COST"]!=0)#.notnull())
            choices_preroll_unit_net_not_null = preroll_final_table["NET_PLANNED_COST"] * preroll_final_table["Currency Exchange Rate"]

            mask_preroll_cost_net_is_null = (~preroll_final_table["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (preroll_final_table["NET_PLANNED_COST"]==0)#.isnull())
            choices_preroll_cost_net_is_null = preroll_final_table["GROSS_PLANNED_COST"] * preroll_final_table["Currency Exchange Rate"]

            preroll_final_table["PLANNED_COST"] = np.select([mask_preroll_cost_au_nz_gb_not_null,
                                                             mask_preroll_cost_au_nz_gb_is_null,
                                                             mask_preroll_cost_net_not_null,
                                                             mask_preroll_cost_net_is_null],
                                                            [choice_preroll_cost_au_nz_gb_not_null,
                                                             choice_preroll_cost_au_nz_gb_is_null,
                                                             choices_preroll_unit_net_not_null,
                                                             choices_preroll_cost_net_is_null],default=0.00)


            conditionscpcv = preroll_final_table["COST_TYPE"].isin(["CPCV"])
            conditionscpm = preroll_final_table["COST_TYPE"].isin(["CPM"])

            choicescpcv = preroll_final_table["COMPLETIONS"]
            choicescpm = preroll_final_table["IMPRESSION"]

            preroll_final_table['Delivered_Impressions'] = np.select([conditionscpcv, conditionscpm],
                                                                     [choicescpcv, choicescpm])

            preroll_access_table = preroll_final_table[["PLACEMENT#", "START_DATE", "END_DATE", "PLACEMENT_NAME",
                                                        "COST_TYPE", "UNIT_COST", "PLANNED_COST",
                                                        "BOOKED_IMP#BOOKED_ENG", "Delivered_Impressions"]]


            self.preroll_access_table = preroll_access_table

    def display_summary_creation(self):
        """
        Creating Summary Sheet
        """

        if self.sqlscript.read_sql__display.empty:
            pass
        else:
            mask_display_spend_cpm = self.displayfirsttable["COST_TYPE"].isin(['CPM'])
            mask_display_spend_cpc = self.displayfirsttable["COST_TYPE"].isin(['CPC'])

            choice_display_spend_cpm = self.displayfirsttable['Delivered_Impressions'] / 1000 * \
                                       self.displayfirsttable['UNIT_COST']
            choice_display_spend_cpc = self.displayfirsttable['Delivered_Impressions'] * self.displayfirsttable[
                'UNIT_COST']

            self.displayfirsttable['Delivery%'] = self.displayfirsttable['Delivered_Impressions'] / \
                                                  self.displayfirsttable[
                                                      'BOOKED_IMP#BOOKED_ENG']

            self.displayfirsttable['Spend'] = np.select([mask_display_spend_cpm, mask_display_spend_cpc],
                                                        [choice_display_spend_cpm, choice_display_spend_cpc])

            self.displayfirsttable["PLACEMENT#"] = self.displayfirsttable["PLACEMENT#"].astype(int)



    def vdx_summary_creation(self):
        """VDX Summary Creation"""

        if self.sqlscript.read_sql__v_d_x.empty:
            pass
        else:
            self.vdx_access_table["Delivered_Engagements"] = self.vdx_access_table["Delivered_Engagements"].astype(
                int)
            self.vdx_access_table["Delivered_Impressions"] = self.vdx_access_table["Delivered_Impressions"].astype(
                int)
            self.vdx_access_table["PLACEMENT#"] = self.vdx_access_table["PLACEMENT#"].astype(int)

            choices_vdx_eng = self.vdx_access_table["Delivered_Engagements"] / self.vdx_access_table[
                "BOOKED_IMP#BOOKED_ENG"]
            choices_vdx_cpcv = self.vdx_access_table["Delivered_Impressions"] / self.vdx_access_table[
                "BOOKED_IMP#BOOKED_ENG"]

            choices_vdx_eng_spend = self.vdx_access_table["Delivered_Engagements"] * self.vdx_access_table[
                "UNIT_COST"]
            choices_vdx_cpcv_spend = self.vdx_access_table["Delivered_Impressions"] * self.vdx_access_table[
                "UNIT_COST"]
            choices_vdx_cpm_spend = self.vdx_access_table["Delivered_Impressions"] / 1000 * self.vdx_access_table[
                "UNIT_COST"]

            mask1 = self.vdx_access_table["COST_TYPE"].isin(['CPE', 'CPE+'])
            mask2 = self.vdx_access_table["COST_TYPE"].isin(['CPM', 'CPCV'])
            mask3 = self.vdx_access_table["COST_TYPE"].isin(['CPCV'])
            mask4 = self.vdx_access_table["COST_TYPE"].isin(['CPM'])

            self.vdx_access_table['Delivery%'] = np.select([mask1, mask2], [choices_vdx_eng, choices_vdx_cpcv],
                                                           default=0.00)

            self.vdx_access_table['Spend'] = np.select([mask1, mask3, mask4], [choices_vdx_eng_spend,
                                                                               choices_vdx_cpcv_spend,
                                                                               choices_vdx_cpm_spend],
                                                       default=0.00)
            self.vdx_access_table['Delivery%'] = self.vdx_access_table['Delivery%'].replace(np.inf, 0.00)
            self.vdx_access_table['Spend'] = self.vdx_access_table['Spend'].replace(np.inf, 0.00)


    def preroll_summary_creation(self):

        """Preroll summary creation"""

        if self.sqlscript.read_sql_preroll.empty:
            pass
        else:
            mask5 = self.preroll_access_table["COST_TYPE"].isin(['CPCV'])
            mask6 = self.preroll_access_table["COST_TYPE"].isin(['CPM'])

            choice_preroll_cpcv = self.preroll_access_table["Delivered_Impressions"] * self.preroll_access_table[
                "UNIT_COST"]
            choice_preroll_cpm = self.preroll_access_table["Delivered_Impressions"] / 1000 * \
                                 self.preroll_access_table["UNIT_COST"]

            self.preroll_access_table["PLACEMENT#"] = self.preroll_access_table["PLACEMENT#"].astype(int)

            self.preroll_access_table['Delivery%'] = self.preroll_access_table["Delivered_Impressions"] / \
                                                     self.preroll_access_table[
                                                         "BOOKED_IMP#BOOKED_ENG"]

            self.preroll_access_table['Spend'] = np.select([mask5, mask6],
                                                           [choice_preroll_cpcv, choice_preroll_cpm])
            self.preroll_access_table['Delivery%'] = self.preroll_access_table['Delivery%'].replace(np.inf, 0.00)
            self.preroll_access_table['Spend'] = self.preroll_access_table['Spend'].replace(np.inf, 0.00)




    def rename_display(self):
        """
        Display Placements Rename Column """

        rename_display = self.displayfirsttable.rename(columns={"PLACEMENT#": "Placement#", "START_DATE": "Start Date",
                                                                "END_DATE": "End Date",
                                                                "PLACEMENT_NAME": "Placement Name",
                                                                "COST_TYPE": "Cost Type", "UNIT_COST": "Unit Cost",
                                                                "PLANNED_COST": "Planned Cost",
                                                                "BOOKED_IMP#BOOKED_ENG": "Booked",
                                                                "Delivered_Impressions": "Delivered_Impressions"},
                                                       inplace=True)

    def rename_vdx(self):
        """
        VDX Placements Rename Column
        """
        rename_vdx = self.vdx_access_table.rename(columns={"PLACEMENT#": "Placement#", "START_DATE": "Start Date",
                                                           "END_DATE": "End Date", "PLACEMENT_NAME": "Placement Name",
                                                           "COST_TYPE": "Cost Type", "UNIT_COST": "Unit Cost",
                                                           "PLANNED_COST": "Planned Cost",
                                                           "BOOKED_IMP#BOOKED_ENG": "Booked"},
                                                  inplace=True)

    def rename_preroll(self):
        """
        Preroll Placements Rename Column
        """
        rename_preroll = self.preroll_access_table.rename(
            columns={"PLACEMENT#": "Placement#", "START_DATE": "Start Date", "END_DATE": "End Date",
                     "PLACEMENT_NAME": "Placement Name",
                     "COST_TYPE": "Cost Type", "UNIT_COST": "Unit Cost",
                     "PLANNED_COST": "Planned Cost", "BOOKED_IMP#BOOKED_ENG": "Booked"},
            inplace=True)

    def write_campaign_info(self):
        """
        Writing Campaign_information to File

        """
        try:
            info_client = self.config.client_info.to_excel(self.config.writer, sheet_name="Delivery Summary",
                                                           startcol=1, startrow=1, index=True, header=False)
            info_campaign = self.config.campaign_info.to_excel(self.config.writer, sheet_name="Delivery Summary",
                                                               startcol=1, startrow=2, index=True, header=False)
            info_ac_mgr = self.config.ac_mgr.to_excel(self.config.writer, sheet_name="Delivery Summary", startcol=4,
                                                      startrow=1, index=True, header=False)
            info_sales_rep = self.config.sales_rep.to_excel(self.config.writer, sheet_name="Delivery Summary",
                                                            startcol=4, startrow=2, index=True, header=False)
            info_campaign_date = self.config.sdate_edate_final.to_excel(self.config.writer,
                                                                        sheet_name="Delivery Summary", startcol=7,
                                                                        startrow=1, index=True, header=False)
            info_agency = self.config.agency_info.to_excel(self.config.writer, sheet_name="Delivery Summary",
                                                           startcol=1, startrow=3, index=True, header=False)
            info_currency = self.config.currency_info.to_excel(self.config.writer, sheet_name="Delivery Summary",
                                                               startcol=7, startrow=3, index=True, header=False)
        except (KeyError, AttributeError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

    def write_summary_display(self):
        """
        Writing Display_Data to File

        """

        display_info = self.displayfirsttable.to_excel(self.config.writer, sheet_name="Delivery Summary",
                                                       startcol=2, startrow=8, header=True, index=False)

    def write_summary_vdx(self):
        """
        Writing VDX_Data to File

        """
        display_length = 0
        if self.displayfirsttable is not None:
            display_length = len(self.displayfirsttable) + 4

        vdx_info = self.vdx_access_table.to_excel(self.config.writer, sheet_name="Delivery Summary",
                                                  startcol=2, startrow=8 + display_length, header=True, index=False)

    def write_summary_preroll(self):
        """

        Writing Preroll_Data to File

        """
        display_length = 0
        if self.displayfirsttable is not None:
            display_length = len(self.displayfirsttable) + 4
        vdx_length = 0
        if self.vdx_access_table is not None:
            vdx_length = len(self.vdx_access_table) + 4

        preroll_info = self.preroll_access_table.to_excel(self.config.writer, sheet_name="Delivery Summary",
                                                          startcol=2, startrow=8 + display_length + vdx_length,
                                                          header=True, index=False)

    def format_campaign_info(self):
        """
        formatting campaign info

        """
        workbook = self.config.writer.book
        worksheet = self.config.writer.sheets["Delivery Summary"]
        worksheet.set_zoom(75)
        worksheet.hide_gridlines(2)
        worksheet.set_row(0, 6)
        worksheet.set_column("A:A", 2)
        format_campaign_info = workbook.add_format({"bold": True, "bg_color": '#00B0F0', "align": "left"})
        worksheet.insert_image("O7", "Exponential.png", {"url": "https://www.tribalfusion.com"})
        worksheet.insert_image("M2", "Client_Logo.png")
        format_write = workbook.add_format({"bold": True, "bg_color": "#00B0F0", "align": "left"})
        format_header = workbook.add_format({"bold": True, "bg_color": "#00B0F0", 'align': 'center'})
        format_subtotal = workbook.add_format({"bg_color": "#A5A5A5", "bold": True, "align": "center"})
        format_subtotal_row = workbook.add_format({"bg_color": "#A5A5A5", "bold": True})
        number_fmt = workbook.add_format({"num_format": "#,##0", "bg_color": "#A5A5A5", "bold": True})
        number_fmt_new = workbook.add_format({"num_format": '#,##0'})
        percent_fmt = workbook.add_format({"num_format": "0.00%", "bg_color": "#A5A5A5", "bold": True})
        percent_fmt_new = workbook.add_format({"num_format": "0.00%"})

        money_fmt_total = workbook.add_format({"num_format": "$#,###0.00", "bg_color": "#A5A5A5", "bold": True})
        money_fmt = workbook.add_format({"num_format": "$#,###0.00"})

        money_fmt_total_mxn = workbook.add_format(
            {"num_format": '"MXN" #,###0.00', "bg_color": "#A5A5A5", "bold": True})
        money_fmt_mxn = workbook.add_format({"num_format": '"MXN" #,###0.00'})

        money_fmt_total_zar = workbook.add_format(
            {"num_format": '"ZAR" #,###0.00', "bg_color": "#A5A5A5", "bold": True})
        money_fmt_zar = workbook.add_format({"num_format": '"ZAR" #,###0.00'})

        money_fmt_total_chf = workbook.add_format(
            {"num_format": '"CHF" #,###0.00', "bg_color": "#A5A5A5", "bold": True})
        money_fmt_chf = workbook.add_format({"num_format": '"CHF" #,###0.00'})

        money_fmt_total_inr = workbook.add_format({"num_format": u"₹#,###0.00", "bg_color": "#A5A5A5", "bold": True})
        money_fmt_inr = workbook.add_format({"num_format": u'₹#,###0.00'})

        money_fmt_total_myr = workbook.add_format(
            {"num_format": '"MYR" #,###0.00', "bg_color": "#A5A5A5", "bold": True})
        money_fmt_myr = workbook.add_format({"num_format": '"MYR" #,###0.00'})

        money_fmt_total_thb = workbook.add_format(
            {"num_format": '"THB" #,###0.00', "bg_color": "#A5A5A5", "bold": True})
        money_fmt_thb = workbook.add_format({"num_format": '"THB" #,###0.00'})

        money_fmt_total_eur = workbook.add_format({"num_format": u"€#,###0.00", "bg_color": "#A5A5A5", "bold": True})
        money_fmt_eur = workbook.add_format({"num_format": u'€#,###0.00'})

        money_fmt_total_gbp = workbook.add_format({"num_format": u"£#,###0.00", "bg_color": "#A5A5A5", "bold": True})
        money_fmt_gbp = workbook.add_format({"num_format": u'£#,###0.00'})

        worksheet.write_string(2, 8, self.config.status)
        worksheet.write_string(2, 7, "Campaign Status")
        # worksheet.write_string (3, 8, "Agency Name")
        # worksheet.write_string (3, 7, "Currency")
        start_row = 7
        start_col = 2
        end_row = 2

        try:
            if self.sqlscript.read_sql__display.empty:
                pass
            else:
                worksheet.write_string(start_row, start_col, "Standard Banners (Performance/Brand)", format_write)
                worksheet.set_row(start_row + 1, 29)

                worksheet.conditional_format(start_row, start_col, start_row, self.displayfirsttable.shape[1] + 1,
                                             {"type": "no_blanks", "format": format_campaign_info})

                worksheet.conditional_format(start_row, start_col, start_row, self.displayfirsttable.shape[1] + 1,
                                             {"type": "blanks", "format": format_campaign_info})

                worksheet.conditional_format(start_row + 1, start_col, start_row + 1,
                                             self.displayfirsttable.shape[1] + 1,
                                             {"type": "no_blanks", "format": format_header})

                worksheet.conditional_format(start_row + 1, start_col, start_row + 1,
                                             self.displayfirsttable.shape[1] + 1,
                                             {"type": "blanks", "format": format_header})

                worksheet.write_string(start_row + self.displayfirsttable.shape[0] + end_row, start_col, "Subtotal",
                                       format_subtotal)

                for col in range(2, 7):
                    startrowformat = start_row + self.displayfirsttable.shape[0] + end_row
                    worksheet.conditional_format(startrowformat, col, startrowformat, col,
                                                 {"type": "no_blanks", "format": format_subtotal_row})
                    worksheet.conditional_format(startrowformat, col, startrowformat, col,
                                                 {"type": "blanks", "format": format_subtotal_row})

                for col in range(7, 8):
                    startrowmoney = start_row + end_row
                    endrowmoney = start_row + self.displayfirsttable.shape[0] + 1

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_zar})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_mxn})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_thb})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_eur})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_gbp})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_inr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_myr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_chf})
                    else:
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt})

                    startrowformat = start_row + self.displayfirsttable.shape[0] + end_row
                    worksheet.conditional_format(startrowformat, col, startrowformat, col,
                                                 {"type": "no_blanks", "format": format_subtotal_row})
                    worksheet.conditional_format(startrowformat, col, startrowformat, col,
                                                 {"type": "blanks", "format": format_subtotal_row})

                for col in range(8, 9):
                    cell_location = xl_rowcol_to_cell(start_row + self.displayfirsttable.shape[0] + end_row, col)
                    start_range = xl_rowcol_to_cell(start_row + end_row, col)
                    end_range = xl_rowcol_to_cell(start_row + self.displayfirsttable.shape[0] + 1, col)
                    formula = '=sum({:s}:{:s})'.format(start_range, end_range)

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_zar)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_mxn)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_thb)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_eur)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_gbp)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_inr)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_myr)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_chf)
                    else:
                        worksheet.write_formula(cell_location, formula, money_fmt_total)

                    startrowmoney = start_row + end_row
                    endrowmoney = start_row + self.displayfirsttable.shape[0] + 1

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_zar})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_mxn})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_thb})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_eur})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_gbp})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_inr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_myr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_chf})
                    else:
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt})

                for col in range(9, 11):
                    cell_location = xl_rowcol_to_cell(start_row + end_row + self.displayfirsttable.shape[0], col)
                    start_range = xl_rowcol_to_cell(start_row + end_row, col)
                    end_range = xl_rowcol_to_cell(start_row + self.displayfirsttable.shape[0] + 1, col)
                    formula = '=sum({:s}:{:s})'.format(start_range, end_range)
                    worksheet.write_formula(cell_location, formula, number_fmt)
                    startrownumber = start_row + end_row
                    endrownumber = start_row + self.displayfirsttable.shape[0] + 1
                    worksheet.conditional_format(startrownumber, col, endrownumber, col,
                                                 {"type": "no_blanks", "format": number_fmt_new})

                worksheet.write_formula(start_row + end_row + self.displayfirsttable.shape[0],
                                        self.displayfirsttable.shape[1],
                                        '=IFERROR(K{}/J{},0)'.format(
                                            start_row + end_row + self.displayfirsttable.shape[0] + 1,
                                            start_row + end_row + self.displayfirsttable.shape[0] + 1), percent_fmt)

                worksheet.conditional_format(start_row + end_row, self.displayfirsttable.shape[1],
                                             start_row + self.displayfirsttable.shape[0] + 1,
                                             self.displayfirsttable.shape[1],
                                             {"type": "no_blanks", "format": percent_fmt_new})

                for col in range(12, 13):
                    cell_location = xl_rowcol_to_cell(start_row + end_row + self.displayfirsttable.shape[0], col)
                    start_range = xl_rowcol_to_cell(start_row + end_row, col)
                    end_range = xl_rowcol_to_cell(start_row + self.displayfirsttable.shape[0] + 1, col)
                    formula = '=sum({:s}:{:s})'.format(start_range, end_range)

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_zar)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_mxn)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_thb)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_eur)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_gbp)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_inr)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_myr)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_chf)
                    else:
                        worksheet.write_formula(cell_location, formula, money_fmt_total)

                    startrowmoney = start_row + end_row
                    endrowmoney = start_row + self.displayfirsttable.shape[0] + 1

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_zar})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_mxn})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_thb})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_eur})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_gbp})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_inr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_myr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_chf})
                    else:
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt})

        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.sqlscript.read_sql__v_d_x.empty:
                pass
            else:
                display_row = 0
                if self.displayfirsttable is not None:
                    display_row = self.displayfirsttable.shape[0] + 4

                worksheet.write_string(start_row + display_row, start_col, "VDX (Display, Mobile and Instream)",
                                       format_write)

                worksheet.conditional_format(start_row + display_row, start_col, start_row + display_row,
                                             self.vdx_access_table.shape[1] + 1,
                                             {"type": "no_blanks", "format": format_campaign_info})

                worksheet.conditional_format(start_row + display_row, start_col, start_row + display_row,
                                             self.vdx_access_table.shape[1] + 1,
                                             {"type": "blanks", "format": format_campaign_info})

                worksheet.conditional_format(start_row + display_row + 1, start_col, start_row + display_row + 1,
                                             self.vdx_access_table.shape[1] + 1,
                                             {"type": "blanks", "format": format_header})

                worksheet.conditional_format(start_row + display_row + 1, start_col, start_row + display_row + 1,
                                             self.vdx_access_table.shape[1] + 1,
                                             {"type": "no_blanks", "format": format_header})

                worksheet.set_row(start_row + display_row + 1, 29)
                worksheet.write_string(start_row + end_row + display_row + self.vdx_access_table.shape[0], start_col,
                                       "Subtotal", format_subtotal)

                for col in range(2, 7):
                    startrowformat = start_row + end_row + display_row + self.vdx_access_table.shape[0]
                    worksheet.conditional_format(startrowformat, col, startrowformat, col,
                                                 {"type": "no_blanks", "format": format_subtotal_row})
                    worksheet.conditional_format(startrowformat, col, startrowformat, col,
                                                 {"type": "blanks", "format": format_subtotal_row})

                    # print(self.config.cdb_value_currency.iloc[0,0])
                    # exit()
                for col in range(7, 8):
                    startrowmoney = start_row + display_row + end_row
                    endrowmoney = start_row + display_row + self.vdx_access_table.shape[0] + 1

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_zar})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_mxn})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_thb})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_eur})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_gbp})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_inr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_myr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_chf})
                    else:
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt})

                    startrowformat = start_row + end_row + display_row + self.vdx_access_table.shape[0]
                    worksheet.conditional_format(startrowformat, col, startrowformat, col,
                                                 {"type": "no_blanks", "format": format_subtotal_row})
                    worksheet.conditional_format(startrowformat, col, startrowformat, col,
                                                 {"type": "blanks", "format": format_subtotal_row})

                for col in range(8, 9):
                    cell_location = xl_rowcol_to_cell(
                        start_row + end_row + display_row + self.vdx_access_table.shape[0], col)
                    start_range = xl_rowcol_to_cell(start_row + display_row + end_row, col)
                    end_range = xl_rowcol_to_cell(start_row + display_row + self.vdx_access_table.shape[0] + 1, col)
                    formula = '=sum({:s}:{:s})'.format(start_range, end_range)

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_zar)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_mxn)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_thb)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_eur)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_gbp)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_inr)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_myr)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_chf)
                    else:
                        worksheet.write_formula(cell_location, formula, money_fmt_total)

                    startrowmoney = start_row + display_row + end_row
                    endrowmoney = start_row + display_row + self.vdx_access_table.shape[0] + 1

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_zar})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_mxn})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_thb})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_eur})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_gbp})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_inr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_myr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_chf})
                    else:
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt})

                for col in range(9, 12):
                    cell_location = xl_rowcol_to_cell(
                        start_row + end_row + display_row + self.vdx_access_table.shape[0], col)
                    start_range = xl_rowcol_to_cell(start_row + display_row + end_row, col)
                    end_range = xl_rowcol_to_cell(start_row + display_row + self.vdx_access_table.shape[0] + 1, col)
                    formula = '=sum({:s}:{:s})'.format(start_range, end_range)
                    worksheet.write_formula(cell_location, formula, number_fmt)
                    startrownumber = start_row + display_row + end_row
                    endrownumber = start_row + display_row + self.vdx_access_table.shape[0] + 1
                    worksheet.conditional_format(startrownumber, col, endrownumber, col,
                                                 {"type": "no_blanks", "format": number_fmt_new})

                for col in range(12, 13):
                    startrowformat = start_row + end_row + display_row + self.vdx_access_table.shape[0]
                    worksheet.conditional_format(startrowformat, col, startrowformat, col,
                                                 {"type": "no_blanks", "format": format_subtotal_row})
                    worksheet.conditional_format(startrowformat, col, startrowformat, col,
                                                 {"type": "blanks", "format": format_subtotal_row})
                    startrownumber = start_row + display_row + end_row
                    endrownumber = start_row + display_row + self.vdx_access_table.shape[0] + 1
                    worksheet.conditional_format(startrownumber, col, endrownumber, col,
                                                 {"type": "no_blanks", "format": percent_fmt_new})

                for col in range(13, 14):
                    cell_location = xl_rowcol_to_cell(
                        start_row + end_row + display_row + self.vdx_access_table.shape[0], col)
                    start_range = xl_rowcol_to_cell(start_row + display_row + end_row, col)
                    end_range = xl_rowcol_to_cell(start_row + display_row + self.vdx_access_table.shape[0] + 1, col)
                    formula = '=sum({:s}:{:s})'.format(start_range, end_range)

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_zar)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_mxn)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_thb)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_eur)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_gbp)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_inr)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_myr)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_chf)
                    else:
                        worksheet.write_formula(cell_location, formula, money_fmt_total)

                    startrowmoney = start_row + display_row + end_row
                    endrowmoney = start_row + display_row + self.vdx_access_table.shape[0] + 1

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_zar})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_mxn})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_thb})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_eur})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_gbp})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_inr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_myr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_chf})
                    else:
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt})

        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.sqlscript.read_sql_preroll.empty:
                pass
            else:
                display_row = 0
                vdx_row = 0
                if self.displayfirsttable is not None:
                    display_row = self.displayfirsttable.shape[0] + 4
                if self.vdx_access_table is not None:
                    vdx_row = self.vdx_access_table.shape[0] + 4

                worksheet.write_string(start_row + display_row + vdx_row, start_col, "Standard Pre Roll", format_write)

                worksheet.conditional_format(start_row + display_row + vdx_row, start_col,
                                             start_row + display_row + vdx_row,
                                             self.preroll_access_table.shape[1] + 1,
                                             {"type": "no_blanks", "format": format_campaign_info})

                worksheet.conditional_format(start_row + display_row + vdx_row, start_col,
                                             start_row + display_row + vdx_row,
                                             self.preroll_access_table.shape[1] + 1,
                                             {"type": "blanks", "format": format_campaign_info})

                worksheet.conditional_format(start_row + display_row + vdx_row + 1, start_col,
                                             start_row + display_row + vdx_row + 1,
                                             self.preroll_access_table.shape[1] + 1,
                                             {"type": "blanks", "format": format_header})

                worksheet.conditional_format(start_row + display_row + vdx_row + 1, start_col,
                                             start_row + display_row + vdx_row + 1,
                                             self.preroll_access_table.shape[1] + 1,
                                             {"type": "no_blanks", "format": format_header})

                worksheet.set_row(start_row + display_row + vdx_row + 1, 29)
                worksheet.write_string(start_row + display_row + vdx_row + self.preroll_access_table.shape[0] + end_row,
                                       start_col, "Subtotal",
                                       format_subtotal)

                for col in range(2, 7):
                    startrowformat = start_row + display_row + vdx_row + end_row + self.preroll_access_table.shape[0]
                    worksheet.conditional_format(startrowformat, col, startrowformat, col,
                                                 {"type": "no_blanks", "format": format_subtotal_row})
                    worksheet.conditional_format(startrowformat, col, startrowformat, col,
                                                 {"type": "blanks", "format": format_subtotal_row})

                for col in range(7, 8):
                    startrowmoney = start_row + display_row + vdx_row + end_row
                    endrowmoney = start_row + display_row + vdx_row + self.preroll_access_table.shape[0] + 1

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_zar})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_mxn})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_thb})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_eur})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_gbp})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_inr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_myr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_chf})

                    else:
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt})

                    startrowformat = start_row + display_row + vdx_row + end_row + self.preroll_access_table.shape[0]
                    worksheet.conditional_format(startrowformat, col, startrowformat, col,
                                                 {"type": "no_blanks", "format": format_subtotal_row})
                    worksheet.conditional_format(startrowformat, col, startrowformat, col,
                                                 {"type": "blanks", "format": format_subtotal_row})

                for col in range(8, 9):
                    cell_location = xl_rowcol_to_cell(
                        start_row + display_row + vdx_row + end_row + self.preroll_access_table.shape[0], col)
                    start_range = xl_rowcol_to_cell(start_row + display_row + vdx_row + end_row, col)
                    end_range = xl_rowcol_to_cell(
                        start_row + display_row + vdx_row + self.preroll_access_table.shape[0] + 1, col)
                    formula = '=sum({:s}:{:s})'.format(start_range, end_range)

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_zar)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_mxn)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_thb)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_eur)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_gbp)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_inr)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_myr)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_chf)
                    else:
                        worksheet.write_formula(cell_location, formula, money_fmt_total)

                    startrowmoney = start_row + display_row + vdx_row + end_row
                    endrowmoney = start_row + display_row + vdx_row + self.preroll_access_table.shape[0] + 1

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_zar})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_mxn})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_thb})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_eur})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_gbp})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_inr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_myr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_chf})
                    else:
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt})

                for col in range(9, 11):
                    cell_location = xl_rowcol_to_cell(
                        start_row + display_row + vdx_row + end_row + self.preroll_access_table.shape[0], col)
                    start_range = xl_rowcol_to_cell(start_row + display_row + vdx_row + end_row, col)
                    end_range = xl_rowcol_to_cell(
                        start_row + display_row + vdx_row + self.preroll_access_table.shape[0] + 1, col)
                    formula = '=sum({:s}:{:s})'.format(start_range, end_range)
                    worksheet.write_formula(cell_location, formula, number_fmt)
                    startrownumber = start_row + display_row + vdx_row + end_row
                    endrownumber = start_row + display_row + vdx_row + self.preroll_access_table.shape[0] + 1
                    worksheet.conditional_format(startrownumber, col, endrownumber, col,
                                                 {"type": "no_blanks", "format": number_fmt_new})

                worksheet.write_formula(
                    start_row + display_row + vdx_row + end_row + self.preroll_access_table.shape[0],
                    self.preroll_access_table.shape[1],
                    '=IFERROR(K{}/J{},0)'.format(
                        start_row + display_row + vdx_row + end_row + self.preroll_access_table.shape[0] + 1,
                        start_row + display_row + vdx_row + end_row + self.preroll_access_table.shape[0] + 1),
                    percent_fmt)

                worksheet.conditional_format(start_row + display_row + vdx_row + end_row,
                                             self.preroll_access_table.shape[1],
                                             start_row + display_row + vdx_row + self.preroll_access_table.shape[0] + 1,
                                             self.preroll_access_table.shape[1],
                                             {"type": "no_blanks", "format": percent_fmt_new})

                for col in range(12, 13):
                    cell_location = xl_rowcol_to_cell(
                        start_row + display_row + vdx_row + end_row + self.preroll_access_table.shape[0], col)
                    start_range = xl_rowcol_to_cell(start_row + display_row + vdx_row + end_row, col)
                    end_range = xl_rowcol_to_cell(
                        start_row + display_row + vdx_row + self.preroll_access_table.shape[0] + 1, col)
                    formula = '=sum({:s}:{:s})'.format(start_range, end_range)

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_zar)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_mxn)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_thb)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_eur)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_gbp)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_inr)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_myr)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.write_formula(cell_location, formula, money_fmt_total_chf)
                    else:
                        worksheet.write_formula(cell_location, formula, money_fmt_total)

                    startrowmoney = start_row + display_row + vdx_row + end_row
                    endrowmoney = start_row + display_row + vdx_row + self.preroll_access_table.shape[0] + 1

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_zar})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_mxn})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_thb})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_eur})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_gbp})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_inr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_myr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt_chf})
                    else:
                        worksheet.conditional_format(startrowmoney, col, endrowmoney, col,
                                                     {"type": "no_blanks", "format": money_fmt})

        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        aligment_left = workbook.add_format({"align": "left"})
        aligment_right = workbook.add_format({"align": "right"})
        aligment_center = workbook.add_format({"align": "center"})
        worksheet.set_column("B:B", 15, aligment_left)
        worksheet.set_column("C:C", 14, aligment_center)
        worksheet.set_column("D:D", 16, aligment_center)
        worksheet.set_column("E:E", 21, aligment_center)
        worksheet.set_column("F:F", 30, aligment_left)
        worksheet.set_column("G:G", 9, aligment_center)
        worksheet.set_column("H:H", 21, aligment_right)
        worksheet.set_column("I:I", 17, aligment_right)
        worksheet.set_column("J:R", 17, aligment_right)
        worksheet.set_row(1, None, aligment_left)
        worksheet.set_row(2, None, aligment_left)
        worksheet.set_row(3, None, aligment_left)
        worksheet.conditional_format("A1:R5", {"type": "blanks", "format": format_campaign_info})
        worksheet.conditional_format("A1:R5", {"type": "no_blanks", "format": format_campaign_info})

    def main(self):
        """
        This is main function.
        """
        self.config.common_columns_summary()

        self.logger.info("Start Creating Summary Sheet for IO - {} ".format(self.config.ioid) + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        if self.sqlscript.read_sql__display.empty:
            self.logger.info("No Display Placements live for IO - {} ".format(self.config.ioid))
            pass
        else:
            self.logger.info("Display Placements found for IO - {} ".format(self.config.ioid))
            self.logger.info("Start Creating Summary for Display Placements at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
            self.access_display_summary()
            self.display_summary_creation()
            self.rename_display()
            self.write_summary_display()
            self.write_campaign_info()
            self.format_campaign_info()
            self.logger.info("Summary for Display Placements Created at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))

        if self.sqlscript.read_sql__v_d_x.empty:
            self.logger.info("No VDX Placements live for IO - {}".format(self.config.ioid))
            pass
        else:
            self.logger.info("VDX Placements found for IO - {} ".format(self.config.ioid))
            self.logger.info("Start Creating Summary for VDX Placements at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
            self.access_vdx_summary()
            self.vdx_summary_creation()
            self.rename_vdx()
            self.write_summary_vdx()
            self.write_campaign_info()
            self.format_campaign_info()
            self.logger.info(
                "Summary for VDX Placements Created at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))

        if self.sqlscript.read_sql_preroll.empty:
            self.logger.info("No Pre-Roll Placements live for IO - {} ".format(self.config.ioid))
            pass
        else:
            self.logger.info("Pre-Roll Placements found for IO - {} ".format(self.config.ioid))
            self.logger.info("Start Creating Summary for Pre-Roll Placements at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
            self.access_preroll_summary()
            self.preroll_summary_creation()
            self.rename_preroll()
            self.write_summary_preroll()
            self.write_campaign_info()
            self.format_campaign_info()
            self.logger.info(
                "Summary for Preroll Placements Created at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))

        self.logger.info("Summary Sheet Created for IO - {}".format(self.config.ioid) + " at " +str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))


if __name__ == "__main__":
    pass
# enable it when running for individual file
# c=config.Config('2018-03-19', 582127,'2018-05-27')
# o=Summary(c)
# o.main()
# c.saveAndCloseWriter()
