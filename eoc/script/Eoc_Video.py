# coding=utf-8
# !/usr/bin/env python
"""
VDX
"""
import datetime
import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell, xl_range
from functools import reduce
import pandas.io.formats.excel
from EOC_Module.eoc.script.SQLScript import SqlScript


# desired_width = 320
pd.set_option("display.max_columns", 10)

pandas.io.formats.excel.header_style = None


class Video(object):
    """
Class for VDX Placements
    """

    def __init__(self, config, sqlscript):
        #super(Video, self).__init__(self)
        self.sqlscript = sqlscript
        self.config = config
        self.logger = self.config.logger
        self.placement_summary_final = None
        self.placement_adsize_final = None
        self.placement_by_video_final = None
        self.video_player_final = None
        self.intractions_clicks_new = None
        self.intractions_intrac_new = None
        self.unique_plc_summary = None

    def access_vdx_placement_columns(self):
        """
        Accessing VDX Placements First Table Columns

        """
        placement_summary_final = None

        if self.sqlscript.read_sql_video_km.empty:
            pass
        else:
            placement_vdx = [self.sqlscript.read_sql__v_d_x, self.sqlscript.read_sql__v_d_x_mv]

            summary_placement_vdx = reduce(lambda left, right: pd.merge(left, right, on='PLACEMENT#'), placement_vdx)
            placement_vdx_summary_new = summary_placement_vdx.loc[:,
                                        ["PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE", "PRODUCT",
                                         "IMPRESSIONS", "ENGAGEMENTS", "DPEENGAGEMENTS",
                                         "ENGCLICKTHROUGH", "DPECLICKTHROUGH", "VWRCLICKTHROUGH",
                                         "ENGTOTALTIMESPENT", "DPETOTALTIMESPENT", "COMPLETIONS",
                                         "ENGINTRACTIVEENGAGEMENTS", "DPEINTRACTIVEENGAGEMENTS",
                                         "VIEW100", "ENG100", "DPE100"]]

            placement_vdx_summary_new["Placement# Name"] = placement_vdx_summary_new[["PLACEMENT#",
                                                                                      "PLACEMENT_NAME"]].apply(
                lambda x: ".".join(x),
                axis=1)

            summary_placement = placement_vdx_summary_new.loc[:, ["Placement# Name", "COST_TYPE", "PRODUCT",
                                                                  "IMPRESSIONS", "ENGAGEMENTS", "DPEENGAGEMENTS",
                                                                  "ENGCLICKTHROUGH", "DPECLICKTHROUGH",
                                                                  "VWRCLICKTHROUGH",
                                                                  "ENGTOTALTIMESPENT", "DPETOTALTIMESPENT",
                                                                  "COMPLETIONS",
                                                                  "ENGINTRACTIVEENGAGEMENTS",
                                                                  "DPEINTRACTIVEENGAGEMENTS",
                                                                  "VIEW100", "ENG100", "DPE100"]]

            ## added by Gaurav - start (Added Clickthrough and Video Completions into the list)

            mask1 = summary_placement["COST_TYPE"].isin(['CPE+'])
            mask2 = summary_placement["COST_TYPE"].isin(['CPE', 'CPM', 'CPCV'])
            mask3 = summary_placement["COST_TYPE"].isin(['CPE+', 'CPE', 'CPM', 'CPCV'])
            mask_vdx_first = summary_placement["COST_TYPE"].isin(['CPCV'])
            mask4 = summary_placement["PRODUCT"].isin(['InStream'])
            mask_vdx_first_vwr_vcr = summary_placement["COST_TYPE"].isin(['CPE+', 'CPE', 'CPM'])
            mask5 = summary_placement["PRODUCT"].isin(['Display', 'Mobile'])
            mask6 = summary_placement['COST_TYPE'].isin(['CPM', 'CPE'])
            mask7 = summary_placement["COST_TYPE"].isin(['CPE+'])
            mask8 = summary_placement["COST_TYPE"].isin(['CPCV'])


            summary_placement['Clickthroughs'] = np.select([mask4, mask2 & mask5, mask1 & mask5],
                                                                         [summary_placement['VWRCLICKTHROUGH'],
                                                                          summary_placement["ENGCLICKTHROUGH"],
                                                                          summary_placement["DPECLICKTHROUGH"]], default=0)

            summary_placement['Video Completions'] = np.select([mask5 & mask6, mask5 & mask7, mask5 & mask8,
                                                                              mask4 & mask_vdx_first_vwr_vcr, mask4 & mask_vdx_first],
                                                                             [summary_placement['ENG100'],
                                                                              summary_placement['DPE100'],
                                                                              summary_placement['COMPLETIONS'],
                                                                              summary_placement['VIEW100'],
                                                                              summary_placement['COMPLETIONS']
                                                                              ])
            ## added by Gaurav - end

            placement_vdx_summary_first = summary_placement.append(summary_placement.sum(numeric_only=True),
                                                                   ignore_index=True)


            placement_vdx_summary_first["COST_TYPE"] = placement_vdx_summary_first["COST_TYPE"].fillna('CPE')

            placement_vdx_summary_first["PRODUCT"] = placement_vdx_summary_first["PRODUCT"].fillna('Grand Total')

            placement_vdx_summary_first["Placement# Name"] = placement_vdx_summary_first["Placement# Name"].fillna(
                'Grand '
                'Total')

            mask1 = placement_vdx_summary_first["COST_TYPE"].isin(['CPE+'])
            choice_deep_engagement = placement_vdx_summary_first['DPEENGAGEMENTS'] / placement_vdx_summary_first[
                'IMPRESSIONS']
            mask2 = placement_vdx_summary_first["COST_TYPE"].isin(['CPE', 'CPM', 'CPCV'])
            choice_engagements = placement_vdx_summary_first["ENGAGEMENTS"] / placement_vdx_summary_first['IMPRESSIONS']

            placement_vdx_summary_first["Engagements Rate"] = np.select([mask1, mask2],
                                                                        [choice_deep_engagement, choice_engagements],
                                                                        default=0.00)
            placement_vdx_summary_first["Engagements Rate"] = placement_vdx_summary_first["Engagements Rate"].replace(
                [np.inf, np.nan], 0.00)

            mask3 = placement_vdx_summary_first["COST_TYPE"].isin(['CPE+', 'CPE', 'CPM', 'CPCV'])
            mask_vdx_first = placement_vdx_summary_first["COST_TYPE"].isin(['CPCV'])
            choice_vwr_ctr = placement_vdx_summary_first['VWRCLICKTHROUGH'] / placement_vdx_summary_first['IMPRESSIONS']

            placement_vdx_summary_first["Viewer CTR"] = np.select([mask3], [choice_vwr_ctr], default=0.00)
            placement_vdx_summary_first["Viewer CTR"] = placement_vdx_summary_first["Viewer CTR"].replace(
                [np.inf, np.nan], 0.00)


            choice_eng_ctr = placement_vdx_summary_first["ENGCLICKTHROUGH"] / placement_vdx_summary_first["ENGAGEMENTS"]
            choice_deep_ctr = placement_vdx_summary_first["DPECLICKTHROUGH"] / placement_vdx_summary_first[
                "DPEENGAGEMENTS"]
            placement_vdx_summary_first["Engager CTR"] = np.select([mask1, mask2], [choice_deep_ctr, choice_eng_ctr],
                                                                   default=0.00)

            placement_vdx_summary_first["Engager CTR"] = placement_vdx_summary_first["Engager CTR"].replace(
                [np.nan, np.inf], 0.00)

            mask4 = placement_vdx_summary_first["PRODUCT"].isin(['InStream'])
            mask_vdx_first_vwr_vcr = placement_vdx_summary_first["COST_TYPE"].isin(['CPE+', 'CPE', 'CPM'])
            choice_vwr_vcr = placement_vdx_summary_first['VIEW100'] / placement_vdx_summary_first['IMPRESSIONS']
            choice_vwr_vcr_vdx_first = placement_vdx_summary_first['COMPLETIONS'] / placement_vdx_summary_first[
                'IMPRESSIONS']

            placement_vdx_summary_first['Viewer VCR'] = np.select([mask4 & mask_vdx_first_vwr_vcr, mask4 & mask_vdx_first],
                                                                  [choice_vwr_vcr, choice_vwr_vcr_vdx_first],
                                                                  default='N/A')
            placement_vdx_summary_first['Viewer VCR'] = pd.to_numeric(placement_vdx_summary_first['Viewer VCR'],
                                                                      errors='coerce')

            mask5 = placement_vdx_summary_first["PRODUCT"].isin(['Display', 'Mobile'])
            mask6 = placement_vdx_summary_first['COST_TYPE'].isin(['CPM', 'CPE'])
            choice_eng_vcr_cpe_cpm = placement_vdx_summary_first['ENG100'] / placement_vdx_summary_first['ENGAGEMENTS']
            mask7 = placement_vdx_summary_first["COST_TYPE"].isin(['CPE+'])
            mask8 = placement_vdx_summary_first["COST_TYPE"].isin(['CPCV'])
            choice_eng_vcr_cpe_plus = placement_vdx_summary_first['DPE100'] / placement_vdx_summary_first[
                'DPEENGAGEMENTS']
            choice_eng_vcr_cpcv = placement_vdx_summary_first['COMPLETIONS'] / placement_vdx_summary_first[
                'ENGAGEMENTS']

            placement_vdx_summary_first['Engager VCR'] = np.select([mask5 & mask6, mask5 & mask7, mask5 & mask8],
                                                                   [choice_eng_vcr_cpe_cpm,
                                                                    choice_eng_vcr_cpe_plus,
                                                                    choice_eng_vcr_cpcv],
                                                                   default='N/A')

            placement_vdx_summary_first['Engager VCR'] = pd.to_numeric(placement_vdx_summary_first['Engager VCR'],
                                                                       errors='coerce')

            choice_int_rate_cpe_plus = placement_vdx_summary_first['DPEINTRACTIVEENGAGEMENTS'] / \
                                       placement_vdx_summary_first[
                                           'DPEENGAGEMENTS']
            choice_int_rate_other_than_cpe_plus = placement_vdx_summary_first['ENGINTRACTIVEENGAGEMENTS'] / \
                                                  placement_vdx_summary_first['ENGAGEMENTS']

            placement_vdx_summary_first['Interaction Rate'] = np.select([mask2, mask1],
                                                                        [choice_int_rate_other_than_cpe_plus,
                                                                         choice_int_rate_cpe_plus],
                                                                        default=0.00)

            placement_vdx_summary_first["Interaction Rate"] = placement_vdx_summary_first["Interaction Rate"].replace(
                [np.inf, np.nan], 0.00)

            choiceatscpe_plus = (
                (placement_vdx_summary_first['DPETOTALTIMESPENT'] / placement_vdx_summary_first[
                    'DPEENGAGEMENTS']) / 1000).apply(
                '{0:.2f}'.format)
            choiceatsotherthancpe_plus = (
                (placement_vdx_summary_first['ENGTOTALTIMESPENT'] / placement_vdx_summary_first[
                    'ENGAGEMENTS']) / 1000).apply(
                '{0:.2f}'.format)

            placement_vdx_summary_first['Active Time Spent'] = np.select([mask2, mask1], [choiceatsotherthancpe_plus,
                                                                                          choiceatscpe_plus],
                                                                         default=0.00)

            placement_vdx_summary_first['Active Time Spent'] = placement_vdx_summary_first['Active Time Spent'].astype(
                float)

            placement_vdx_summary_first["Active Time Spent"] = placement_vdx_summary_first["Active Time Spent"].replace(
                [np.inf, np.nan], 0.00)

            placement_vdx_summary_first_new = placement_vdx_summary_first.replace(np.nan, 'N/A', regex=True)

            placement_vdx_summary_first_new.loc[
                placement_vdx_summary_first_new.index[-1], ["Viewer VCR", "Engager VCR"]] = np.nan


            ## edited by Gaurav - start (Added Clickthrough and Video Completions into the list)

            placement_summary_final_new = placement_vdx_summary_first_new.loc[:,
                                          ["Placement# Name", "PRODUCT", "IMPRESSIONS", "ENGAGEMENTS", "Engagements Rate", "Clickthroughs",
                                            "Viewer CTR", "Engager CTR", "Video Completions", "Viewer VCR", "Engager VCR",
                                           "Interaction Rate", "Active Time Spent"]]

            placement_summary_final = placement_summary_final_new.loc[:,
                                      ["Placement# Name", "PRODUCT", "IMPRESSIONS", "ENGAGEMENTS", "Engagements Rate", "Clickthroughs",
                                       "Viewer CTR", "Engager CTR", "Video Completions", "Viewer VCR",
                                       "Engager VCR", "Interaction Rate",
                                       "Active Time Spent"]]

            ## edited by Gaurav - end

            self.placement_summary_final = placement_summary_final

    def access_vdx_adsize_columns(self):

        """Access VDX Adsize Columns"""

        placement_adsize_final = None

        if self.sqlscript.read_sql_adsize_km.empty:
            pass
        else:
            placement_adsize = [self.sqlscript.read_sql__v_d_x, self.sqlscript.read_sql_adsize_km]
            placement_adsize_summary = reduce(lambda left, right: pd.merge(left, right, on='PLACEMENT#'),
                                              placement_adsize)
            placement_adsize_first = placement_adsize_summary.loc[:,
                                     ["PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE", "PRODUCT", "ADSIZE",
                                      "IMPRESSIONS", "ENGAGEMENTS", "DPEENGAGEMENTS",
                                      "ENGCLICKTHROUGHS", "DPECLICKTHROUGHS", "VWRCLICKTHROUGHS",
                                      "VIEW100", "ENG100", "DPE100", "ENGTOTALTIMESPENT",
                                      "DPETOTALTIMESPENT", "ENGINTRACTIVEENGAGEMENTS",
                                      "COMPLETIONS", "DPEINTRACTIVEENGAGEMENTS"]]

            placement_adsize_first["Placement# Name"] = placement_adsize_first[["PLACEMENT#",
                                                                                "PLACEMENT_NAME"]].apply(
                lambda x: ".".join
                (x),
                axis=1)

            placement_adsize_table = placement_adsize_first.loc[:, ["Placement# Name", "COST_TYPE", "PRODUCT", "ADSIZE",
                                                                    "IMPRESSIONS", "ENGAGEMENTS", "DPEENGAGEMENTS",
                                                                    "ENGCLICKTHROUGHS", "DPECLICKTHROUGHS",
                                                                    "VWRCLICKTHROUGHS",
                                                                    "VIEW100", "ENG100", "DPE100", "ENGTOTALTIMESPENT",
                                                                    "DPETOTALTIMESPENT", "ENGINTRACTIVEENGAGEMENTS",
                                                                    "COMPLETIONS", "DPEINTRACTIVEENGAGEMENTS"]]

            placement_adsize_grouping = pd.pivot_table(placement_adsize_table,
                                                       index=['Placement# Name', 'ADSIZE', 'COST_TYPE'],
                                                       values=["IMPRESSIONS", "ENGAGEMENTS", "DPEENGAGEMENTS",
                                                               "ENGCLICKTHROUGHS", "DPECLICKTHROUGHS",
                                                               "VWRCLICKTHROUGHS",
                                                               "VIEW100", "ENG100", "DPE100", "ENGTOTALTIMESPENT",
                                                               "DPETOTALTIMESPENT",
                                                               "ENGINTRACTIVEENGAGEMENTS",
                                                               "COMPLETIONS",
                                                               "DPEINTRACTIVEENGAGEMENTS"], aggfunc=np.sum)

            placement_adsize_grouping_new = placement_adsize_grouping.reset_index()

            placement_adsize_group = placement_adsize_grouping_new.loc[:, :]


            ## added by Gaurav - start (Added Clickthrough and Video Completions into the list)

            mask9 = placement_adsize_group["COST_TYPE"].isin(["CPE+"])
            mask10 = placement_adsize_group["COST_TYPE"].isin(["CPE", "CPM", "CPCV"])
            mask11 = placement_adsize_group["COST_TYPE"].isin(["CPE", "CPM", "CPCV", "CPE+"])
            mask12 = placement_adsize_group["ADSIZE"].isin(["1x10"])
            mask13 = placement_adsize_group["COST_TYPE"].isin(["CPE", "CPE+", "CPM"])
            mask14 = placement_adsize_group["COST_TYPE"].isin(["CPCV"])
            mask15 = ~placement_adsize_group["ADSIZE"].isin(["1x10"])
            mask16 = placement_adsize_group["COST_TYPE"].isin(["CPE", "CPM"])

            placement_adsize_group['Clickthroughs'] = np.select([mask12, mask10 & mask15, mask9 & mask15],
                                                           [placement_adsize_group['VWRCLICKTHROUGHS'],
                                                            placement_adsize_group["ENGCLICKTHROUGHS"],
                                                            placement_adsize_group["DPECLICKTHROUGHS"]], default=0)

            placement_adsize_group['Video Completions'] = np.select([mask10 & mask15, mask15 & mask9, mask14 & mask11,
                                                                mask12 & mask13, mask12 & mask14],
                                                               [placement_adsize_group['ENG100'],
                                                                placement_adsize_group['DPE100'],
                                                                placement_adsize_group['COMPLETIONS'],
                                                                placement_adsize_group['VIEW100'],
                                                                placement_adsize_group['COMPLETIONS']
                                                                ])
            ## added by Gaurav - end


            placement_adsize_group = placement_adsize_group.append(placement_adsize_group.sum(numeric_only=True),
                                                                   ignore_index=True)


            placement_adsize_group["COST_TYPE"] = placement_adsize_group["COST_TYPE"].fillna('CPE')
            placement_adsize_group["ADSIZE"] = placement_adsize_group["ADSIZE"].fillna('Grand Total')
            placement_adsize_group["Placement# Name"] = placement_adsize_group["Placement# Name"].fillna('Grand Total')

            mask9 = placement_adsize_group["COST_TYPE"].isin(["CPE+"])
            choice_adsize_engagement_cpe_plus = placement_adsize_group["DPEENGAGEMENTS"] / placement_adsize_group[
                "IMPRESSIONS"]
            mask10 = placement_adsize_group["COST_TYPE"].isin(["CPE", "CPM", "CPCV"])
            choice_placement_adsize_group_cpe = placement_adsize_group["ENGAGEMENTS"] / placement_adsize_group[
                "IMPRESSIONS"]
            placement_adsize_group["Engagements Rate"] = np.select([mask9, mask10], [choice_adsize_engagement_cpe_plus,
                                                                                     choice_placement_adsize_group_cpe],
                                                                   default=0.00)

            placement_adsize_group["Engagements Rate"] = placement_adsize_group["Engagements Rate"].replace(
                [np.inf, np.nan], 0.00)

            mask11 = placement_adsize_group["COST_TYPE"].isin(["CPE", "CPM", "CPCV", "CPE+"])

            choice_adsize_engagement_vwr_ctr = placement_adsize_group["VWRCLICKTHROUGHS"] / placement_adsize_group[
                "IMPRESSIONS"]

            placement_adsize_group["Viewer CTR"] = np.select([mask11], [choice_adsize_engagement_vwr_ctr], default=0.00)

            placement_adsize_group["Viewer CTR"] = placement_adsize_group["Viewer CTR"].replace([np.inf, np.nan], 0.00)

            choice_adsize_engagement_eng_ctr = placement_adsize_group["ENGCLICKTHROUGHS"] / placement_adsize_group[
                "ENGAGEMENTS"]
            choice_adsize_engagement_dpe_eng_ctr = placement_adsize_group["DPECLICKTHROUGHS"] / placement_adsize_group[
                "DPEENGAGEMENTS"]

            placement_adsize_group["Engager CTR"] = np.select([mask10, mask9], [choice_adsize_engagement_eng_ctr,
                                                                                choice_adsize_engagement_dpe_eng_ctr],
                                                              default=0.00)

            placement_adsize_group["Engager CTR"] = placement_adsize_group["Engager CTR"].replace([np.inf, np.nan],
                                                                                                  0.00)

            mask12 = placement_adsize_group["ADSIZE"].isin(["1x10"])
            mask13 = placement_adsize_group["COST_TYPE"].isin(["CPE", "CPE+", "CPM"])
            mask14 = placement_adsize_group["COST_TYPE"].isin(["CPCV"])
            choice_adsize_vwr_vcr_cpe = (
            placement_adsize_group["VIEW100"] / placement_adsize_group["IMPRESSIONS"]).replace(
                [np.inf, np.nan], 0.00)
            choice_adsize_vwr_vcr_cpcv = (
                placement_adsize_group["COMPLETIONS"] / placement_adsize_group["IMPRESSIONS"]).replace([np.inf, np.nan],
                                                                                                       0.00)
            placement_adsize_group["Viewer VCR"] = np.select([mask12 & mask13, mask12 & mask14],
                                                             [choice_adsize_vwr_vcr_cpe,
                                                              choice_adsize_vwr_vcr_cpcv],
                                                             default='N/A')

            placement_adsize_group['Viewer VCR'] = pd.to_numeric(placement_adsize_group['Viewer VCR'], errors='coerce')

            mask15 = ~placement_adsize_group["ADSIZE"].isin(["1x10"])
            mask16 = placement_adsize_group["COST_TYPE"].isin(["CPE", "CPM"])
            choice_adsize_eng_vcr_cpe = (
            placement_adsize_group["ENG100"] / placement_adsize_group["ENGAGEMENTS"]).replace(
                [np.inf, np.nan], 0.00)
            choiceadsizeengvcrcpe_plus = (
                placement_adsize_group["DPE100"] / placement_adsize_group["DPEENGAGEMENTS"]).replace([np.inf, np.nan],
                                                                                                     0.00)
            choiceadsizeengvcrcpcv = (
                placement_adsize_group["COMPLETIONS"] / placement_adsize_group["ENGAGEMENTS"]).replace([np.inf, np.nan],
                                                                                                       0.00)

            placement_adsize_group["Engager VCR"] = np.select([mask15 & mask16, mask15 & mask9, mask15 & mask14],
                                                              [choice_adsize_eng_vcr_cpe, choiceadsizeengvcrcpe_plus,
                                                               choiceadsizeengvcrcpcv], default='N/A')

            placement_adsize_group['Engager VCR'] = pd.to_numeric(placement_adsize_group['Engager VCR'],
                                                                  errors='coerce')

            choice_adsize_interaction_rate_cpe = placement_adsize_group["ENGINTRACTIVEENGAGEMENTS"] / \
                                                 placement_adsize_group[
                                                     "ENGAGEMENTS"]
            choice_adsize_interaction_rate_cpe_plus = placement_adsize_group["DPEINTRACTIVEENGAGEMENTS"] / \
                                                      placement_adsize_group[
                                                          "DPEENGAGEMENTS"]

            placement_adsize_group["Interaction Rate"] = np.select([mask10, mask9], [choice_adsize_interaction_rate_cpe,
                                                                                     choice_adsize_interaction_rate_cpe_plus],
                                                                   default=0.00)

            placement_adsize_group["Interaction Rate"] = placement_adsize_group["Interaction Rate"].replace(
                [np.inf, np.nan], 0.00)

            choice_adsize_ats_cpe = (
                (placement_adsize_group["ENGTOTALTIMESPENT"] / placement_adsize_group["ENGAGEMENTS"]) / 1000).apply(
                '{0:.2f}'.format)
            choice_adsize_ats_cpe_plus = (
                (placement_adsize_group["DPETOTALTIMESPENT"] / placement_adsize_group["DPEENGAGEMENTS"]) / 1000).apply(
                '{0:.2f}'.format)

            placement_adsize_group["Active Time Spent"] = np.select([mask10, mask14],
                                                                    [choice_adsize_ats_cpe, choice_adsize_ats_cpe_plus],
                                                                    default=0.00)

            placement_adsize_group['Active Time Spent'] = placement_adsize_group['Active Time Spent'].astype(float)

            placement_adsize_group["Active Time Spent"] = placement_adsize_group["Active Time Spent"].replace(
                [np.inf, np.nan], 0.00)

            placement_adsize_group_first_new = placement_adsize_group.replace(np.nan, 'N/A', regex=True)

            placement_adsize_group_first_new.loc[
                placement_adsize_group_first_new.index[-1], ["Viewer VCR", "Engager VCR"]] = \
                np.nan

            placement_adsize_final = placement_adsize_group_first_new.loc[:, ["Placement# Name", "ADSIZE", "IMPRESSIONS", "ENGAGEMENTS",
                                                                              "Engagements Rate", "Clickthroughs", "Viewer CTR",
                                                                              "Engager CTR", "Video Completions", "Viewer VCR",
                                                                              "Engager VCR", "Interaction Rate",
                                                                              "Active Time Spent"]]

            self.placement_adsize_final = placement_adsize_final

    def vdx_video_details(self):

        """Vdx Video Details"""

        # placement_by_video_final = None

        if self.sqlscript.read_sql_video_km.empty:
            pass
        else:
            placement_video = [self.sqlscript.read_sql__v_d_x, self.sqlscript.read_sql_video_km]
            placement_video_summary = reduce(lambda left, right: pd.merge(left, right, on='PLACEMENT#'),
                                             placement_video)

            placement_by_video = placement_video_summary.loc[:,
                                 ["PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE", "PRODUCT",
                                  "VIDEONAME", "VIEW0", "VIEW25", "VIEW50", "VIEW75",
                                  "VIEW100",
                                  "ENG0", "ENG25", "ENG50", "ENG75", "ENG100", "DPE0",
                                  "DPE25",
                                  "DPE50", "DPE75", "DPE100"]]

            placement_by_video["Placement# Name"] = placement_by_video[["PLACEMENT#",
                                                                        "PLACEMENT_NAME"]].apply(
                lambda x: ".".join(x),
                axis=1)

            placement_by_video_new = placement_by_video.loc[:,
                                     ["PLACEMENT#", "Placement# Name", "COST_TYPE", "PRODUCT", "VIDEONAME",
                                      "VIEW0", "VIEW25", "VIEW50", "VIEW75", "VIEW100",
                                      "ENG0", "ENG25", "ENG50", "ENG75", "ENG100", "DPE0", "DPE25",
                                      "DPE50", "DPE75", "DPE100"]]

            placement_by_km_video = [placement_by_video_new, self.sqlscript.read_sql_km_for_video]
            placement_by_km_video_summary = reduce(
                lambda left, right: pd.merge(left, right, on=['PLACEMENT#', 'PRODUCT']),
                placement_by_km_video)

            """Conditions for 25%view"""
            mask17 = placement_by_km_video_summary["PRODUCT"].isin(['Display', 'Mobile'])
            mask18 = placement_by_km_video_summary["COST_TYPE"].isin(["CPE", "CPM", "CPCV"])
            mask19 = placement_by_km_video_summary["PRODUCT"].isin(["InStream"])
            mask20 = placement_by_km_video_summary["COST_TYPE"].isin(["CPE", "CPM", "CPE+", "CPCV"])
            mask_video_video_completions = placement_by_km_video_summary["COST_TYPE"].isin(["CPCV"])
            mask21 = placement_by_km_video_summary["COST_TYPE"].isin(["CPE+"])
            mask22 = placement_by_km_video_summary["COST_TYPE"].isin(["CPE", "CPM"])
            mask23 = placement_by_km_video_summary["PRODUCT"].isin(['Display', 'Mobile', 'InStream'])
            mask24 = placement_by_km_video_summary["COST_TYPE"].isin(["CPE", "CPM", "CPE+"])

            choice25video_eng = placement_by_km_video_summary["ENG25"]
            choice25video_vwr = placement_by_km_video_summary["VIEW25"]
            choice25video_deep = placement_by_km_video_summary["DPE25"]

            placement_by_km_video_summary["25_pc_video"] = np.select(
                [mask17 & mask18, mask19 & mask20, mask17 & mask21],
                [choice25video_eng, choice25video_vwr,
                 choice25video_deep])

            """Conditions for 50%view"""
            choice50video_eng = placement_by_km_video_summary["ENG50"]
            choice50video_vwr = placement_by_km_video_summary["VIEW50"]
            choice50video_deep = placement_by_km_video_summary["DPE50"]

            placement_by_km_video_summary["50_pc_video"] = np.select(
                [mask17 & mask18, mask19 & mask20, mask17 & mask21],
                [choice50video_eng,
                 choice50video_vwr, choice50video_deep])

            """Conditions for 75%view"""

            choice75video_eng = placement_by_km_video_summary["ENG75"]
            choice75video_vwr = placement_by_km_video_summary["VIEW75"]
            choice75video_deep = placement_by_km_video_summary["DPE75"]

            placement_by_km_video_summary["75_pc_video"] = np.select(
                [mask17 & mask18, mask19 & mask20, mask17 & mask21],
                [choice75video_eng,
                 choice75video_vwr,
                 choice75video_deep])

            """Conditions for 100%view"""

            choice100video_eng = placement_by_km_video_summary["ENG100"]
            choice100video_vwr = placement_by_km_video_summary["VIEW100"]
            choice100video_deep = placement_by_km_video_summary["DPE100"]
            choice_completions = placement_by_km_video_summary['COMPLETIONS']

            placement_by_km_video_summary["100_pc_video"] = np.select(
                [mask17 & mask22, mask19 & mask24, mask17 & mask21, mask23 & mask_video_video_completions],
                [choice100video_eng, choice100video_vwr, choice100video_deep, choice_completions])

            """conditions for 0%view"""

            choice0video_eng = placement_by_km_video_summary["ENG0"]
            choice0video_vwr = placement_by_km_video_summary["VIEW0"]
            choice0video_deep = placement_by_km_video_summary["DPE0"]

            placement_by_km_video_summary["Views"] = np.select([mask17 & mask18, mask19 & mask20, mask17 & mask21],
                                                               [choice0video_eng,
                                                                choice0video_vwr,
                                                                choice0video_deep])

            placement_by_video_summary = placement_by_km_video_summary.loc[:,
                                         ["PLACEMENT#", "Placement# Name", "PRODUCT", "VIDEONAME", "COST_TYPE",
                                          "Views", "25_pc_video", "50_pc_video", "75_pc_video", "100_pc_video",
                                          "ENGAGEMENTS", "IMPRESSIONS", "DPEENGAGEMENTS"]]

            # print(placement_by_video_summary[['PLACEMENT#','PRODUCT','Views','100_pc_video',"ENGAGEMENTS", "IMPRESSIONS", "DPEENGAGEMENTS"]])
            # exit()



            """adding views based on conditions"""

            placement_by_video_summary_new = placement_by_km_video_summary.loc[
                placement_by_km_video_summary.reset_index().groupby(['PLACEMENT#', 'PRODUCT'])['Views'].idxmax()]

            # print(placement_by_video_summary_new[['PLACEMENT#','PRODUCT','Views']])
            # exit()



            placement_by_video_summary_new.loc[mask17 & mask18, 'Views'] = placement_by_video_summary_new[
                'ENGAGEMENTS']
            placement_by_video_summary_new.loc[mask19 & mask20, 'Views'] = placement_by_video_summary_new[
                'IMPRESSIONS']
            placement_by_video_summary_new.loc[mask17 & mask21, 'Views'] = placement_by_video_summary_new[
                'DPEENGAGEMENTS']

            placement_by_video_summary = placement_by_video_summary.drop(placement_by_video_summary_new.index).append(
                placement_by_video_summary_new, sort=True).sort_index()

            # print(placement_by_video_summary[["Placement# Name", "PRODUCT", "VIDEONAME", "Views",'ENGAGEMENTS','IMPRESSIONS','DPEENGAGEMENTS']])
            # exit()

            placement_by_video_summary["Video Completion Rate"] = placement_by_video_summary["100_pc_video"] / \
                                                                  placement_by_video_summary["Views"]

            placement_by_video_summary["Video Completion Rate"] = placement_by_video_summary[
                "Video Completion Rate"].replace([np.inf, np.nan], 0.00)

            placement_by_video_final = placement_by_video_summary.loc[:,
                                       ["Placement# Name", "PRODUCT", "VIDEONAME", "Views",
                                        "25_pc_video", "50_pc_video", "75_pc_video", "100_pc_video",
                                        "Video Completion Rate"]]

            placement_by_video_final.sort_values(['Placement# Name', 'PRODUCT', 'Views'], ascending=[True, True, False],
                                                 inplace=True)

            # print(placement_by_video_final)
            # exit()

            self.placement_by_video_final = placement_by_video_final

    def vdx_player_interaction(self):
        """Vdx Player Interaction"""

        video_player_final = None

        if self.sqlscript.read_sql_video_player_interaction.empty:
            pass
        else:

            video_intraction = [self.sqlscript.read_sql__v_d_x, self.sqlscript.read_sql_video_player_interaction]
            video_intraction_final = reduce(lambda left, right: pd.merge(left, right, on=['PLACEMENT#']),
                                            video_intraction)

            video_player = video_intraction_final.loc[:,
                           ["PRODUCT", "VWRMUTE", "VWRUNMUTE", "VWRPAUSE", "VWRREWIND",
                            "VWRRESUME", "VWRREPLAY", "VWRFULLSCREEN", "ENGMUTE",
                            "ENGUNMUTE", "ENGPAUSE", "ENGREWIND", "ENGRESUME", "ENGREPLAY",
                            "ENGFULLSCREEN"]]

            mask22 = video_player["PRODUCT"].isin(['Display', 'Mobile'])
            mask23 = video_player["PRODUCT"].isin(['InStream'])
            choice_intraction_mute = video_player["ENGMUTE"]
            choice_intraction_un_mute = video_player["ENGUNMUTE"]
            choice_intraction_pause = video_player["ENGPAUSE"]
            choice_intraction_rewind = video_player["ENGREWIND"]
            choice_intraction_resume = video_player["ENGRESUME"]
            choice_intraction_replay = video_player["ENGREPLAY"]
            choice_intraction_full_screen = video_player["ENGFULLSCREEN"]
            choice_interaction_ins_mute = video_player["VWRMUTE"]
            choice_interaction_ins_un_mute = video_player["VWRUNMUTE"]
            choice_interaction_ins_pause = video_player["VWRPAUSE"]
            choice_interaction_ins_rewind = video_player["VWRREWIND"]
            choice_interaction_ins_resume = video_player["VWRRESUME"]
            choice_interaction_ins_replay = video_player["VWRREPLAY"]
            choice_interaction_ins_full_screen = video_player["VWRFULLSCREEN"]

            video_player["Mute"] = np.select([mask22, mask23],
                                             [choice_intraction_mute, choice_interaction_ins_mute])
            video_player["Unmute"] = np.select([mask22, mask23], [choice_intraction_un_mute,
                                                                  choice_interaction_ins_un_mute])
            video_player["Pause"] = np.select([mask22, mask23],
                                              [choice_intraction_pause, choice_interaction_ins_pause])
            video_player["Rewind"] = np.select([mask22, mask23], [choice_intraction_rewind,
                                                                  choice_interaction_ins_rewind])
            video_player["Resume"] = np.select([mask22, mask23], [choice_intraction_resume,
                                                                  choice_interaction_ins_resume])
            video_player["Replay"] = np.select([mask22, mask23], [choice_intraction_replay,
                                                                  choice_interaction_ins_replay])
            video_player["Fullscreen"] = np.select([mask22, mask23],
                                                   [choice_intraction_full_screen,
                                                    choice_interaction_ins_full_screen])

            video_player.rename(columns={"PRODUCT": "Product"}, inplace=True)

            vdx_video_player = pd.pivot_table(video_player, index='Product', values=["Mute", "Unmute", "Pause",
                                                                                     "Rewind", "Resume", "Replay",
                                                                                     "Fullscreen"], aggfunc=np.sum)

            vdx_video_player_new = vdx_video_player.reset_index()
            vdx_video_player_r = vdx_video_player_new.loc[:, :]

            video_player_final = vdx_video_player_r.loc[:,
                                 ["Product", "Mute", "Unmute", "Pause", "Rewind", "Resume", "Replay", "Fullscreen"]]

            self.video_player_final = video_player_final

    def vdx_ad_interaction(self):
        """VDX Ad Interaction"""

        intractions_clicks_new = None
        intractions_intrac_new = None
        try:
            if self.sqlscript.read_sql_ad_intraction.empty:
                pass
            else:

                interaction_click_thru = [self.sqlscript.read_sql__v_d_x, self.sqlscript.read_sql_ad_intraction]

                click_through = reduce(lambda left, right: pd.merge(left, right, on=['PLACEMENT#']),
                                       interaction_click_thru)

                intractions_click_ad = pd.pivot_table(click_through, index="PRODUCT", values="CLICKTHRU",
                                                      columns="BLAZE_TAG_NAME_DESC",
                                                      aggfunc=np.sum, fill_value=0)

                intractions_click_ad_new = intractions_click_ad.reset_index()
                intractions_clicks = intractions_click_ad_new.loc[:, :]

                cols_drop = ["PRODUCT"]
                intractions_clicks_new = intractions_clicks.drop(cols_drop, axis=1)

                intractions_clicks_new = intractions_clicks_new.loc[:, (intractions_clicks_new != 0).any(axis=0)]

                intractions_intrac_ad = pd.pivot_table(click_through, index="PRODUCT",
                                                       values="INTERACTION",
                                                       columns="BLAZE_TAG_NAME_DESC", aggfunc=np.sum, fill_value=0)

                intractions_intrac_ad_new = intractions_intrac_ad.reset_index()
                intractions_intrac = intractions_intrac_ad_new.loc[:, :]
                intractions_intrac_new = intractions_intrac.drop(cols_drop, axis=1)

                intractions_intrac_new = intractions_intrac_new.loc[:, (intractions_intrac_new != 0).any(axis=0)]

        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        self.intractions_clicks_new = intractions_clicks_new
        self.intractions_intrac_new = intractions_intrac_new

    def vdx_day_data(self):
        """Creating VDX By day Information"""

        vdx_tables_by_day = [self.sqlscript.read_sql__v_d_x, self.sqlscript.read_sql_video_day]
        summary_data = reduce(lambda left, right: pd.merge(left, right, on=['PLACEMENT#']),vdx_tables_by_day)
        vdx_columns = summary_data.loc[:,["PLACEMENT#","PLACEMENT_NAME","COST_TYPE","PRODUCT","DAY","IMPRESSIONS",
                                          "ENGAGEMENTS","DPEENGAGEMENTS","COMPLETIONS","VIEW100","ENG100","DPE100",
                                          "ENGGERCLICKTHROUGH","VWRCLICKTHROUGH","DPECLICKTHROUGH"]]

        #start adding columns in data frame
        vdx_columns["Placement# Name"] = vdx_columns[["PLACEMENT#","PLACEMENT_NAME"]].apply(lambda x: ".".join(x),axis=1)

        #value selection Impressions
        vdx_value_first_case = vdx_columns["PRODUCT"].isin(['Display', 'Mobile','InStream']) & vdx_columns["COST_TYPE"].isin(['CPE', 'CPM','CPE+','CPCV'])
        choice_vdx_value_first_case = vdx_columns["IMPRESSIONS"]

        vdx_columns["Impressions"] = np.select([vdx_value_first_case],[choice_vdx_value_first_case])

        # value selection Engagements
        vdx_value_second_case = vdx_columns["PRODUCT"].isin(['Display', 'Mobile','InStream']) & vdx_columns["COST_TYPE"].isin(['CPE', 'CPM','CPCV'])
        vdx_value_third_case = vdx_columns["PRODUCT"].isin(['InStream']) & vdx_columns["COST_TYPE"].isin(['CPE+'])

        choice_vdx_value_second_case = vdx_columns["ENGAGEMENTS"]
        choice_vdx_value_third_case = vdx_columns["DPEENGAGEMENTS"]

        vdx_columns['Engagements'] = np.select([vdx_value_second_case,vdx_value_third_case],[choice_vdx_value_second_case,choice_vdx_value_third_case])


        # values selection Enagagements Rate
        vdx_columns['Engagement Rate'] = vdx_columns['Engagements']/vdx_columns["Impressions"]

        # values selection click Throughs

        vdx_value_fourth_case = vdx_columns["PRODUCT"].isin(['Display', 'Mobile']) & vdx_columns["COST_TYPE"].isin(['CPE', 'CPM','CPCV'])
        vdx_value_fifth_case = vdx_columns["PRODUCT"].isin(['InStream']) & vdx_columns["COST_TYPE"].isin(['CPE', 'CPM','CPE+','CPCV'])
        vdx_value_sixth_case = vdx_columns["PRODUCT"].isin(['Display', 'Mobile']) & vdx_columns["COST_TYPE"].isin(['CPE+'])

        choice_vdx_value_fourth_case = vdx_columns['ENGGERCLICKTHROUGH']
        choice_vdx_value_fifth_case = vdx_columns['VWRCLICKTHROUGH']
        choice_vdx_value_sixth_case = vdx_columns['DPECLICKTHROUGH']


        vdx_columns['Clickthroughs'] = np.select([vdx_value_fourth_case,vdx_value_fifth_case,vdx_value_sixth_case],
                                                 [choice_vdx_value_fourth_case,choice_vdx_value_fifth_case,choice_vdx_value_sixth_case])


        # values selection for CTR
        choice_vdx_fourth_case_ctr = vdx_columns['Clickthroughs']/vdx_columns['ENGAGEMENTS']
        choice_vdx_fifth_case_ctr = vdx_columns['Clickthroughs']/vdx_columns['IMPRESSIONS']
        choice_vdx_sixth_case_ctr = vdx_columns['Clickthroughs']/vdx_columns["DPEENGAGEMENTS"]


        vdx_columns['CTR'] = np.select([vdx_value_fourth_case,vdx_value_fifth_case,vdx_value_sixth_case],
                                       [choice_vdx_fourth_case_ctr, choice_vdx_fifth_case_ctr,choice_vdx_sixth_case_ctr])


        # values selection for Video Completions
        vdx_value_seventh_case = vdx_columns["PRODUCT"].isin(['Display', 'Mobile']) & vdx_columns["COST_TYPE"].isin(['CPE', 'CPM'])
        vdx_value_eight_case = vdx_columns["PRODUCT"].isin(['InStream']) & vdx_columns["COST_TYPE"].isin(['CPE', 'CPM','CPE+'])
        vdx_value_ninth_case = vdx_columns["PRODUCT"].isin(['Display', 'Mobile','InStream']) & vdx_columns["COST_TYPE"].isin(['CPCV'])
        vdx_value_tenth_case = vdx_columns["PRODUCT"].isin(['Display', 'Mobile']) & vdx_columns["COST_TYPE"].isin(['CPE+'])

        choice_vdx_seventh_case = vdx_columns['ENG100']
        choice_vdx_eighth_case = vdx_columns['VIEW100']
        choice_vdx_ninth_case = vdx_columns['COMPLETIONS']
        choice_vdx_tenth_case = vdx_columns['DPE100']

        vdx_columns['Video Completions'] = np.select([vdx_value_seventh_case,vdx_value_eight_case,vdx_value_ninth_case,vdx_value_tenth_case],
                                                     [choice_vdx_seventh_case,choice_vdx_eighth_case,choice_vdx_ninth_case,choice_vdx_tenth_case])


        # values selection for Video Completions Rate

        vdx_value_eleventh_case = vdx_columns["PRODUCT"].isin(['Display', 'Mobile']) & vdx_columns["COST_TYPE"].isin(['CPCV'])
        vdx_value_tweleth_case = vdx_columns["PRODUCT"].isin(['InStream']) & vdx_columns["COST_TYPE"].isin(['CPCV'])

        choice_vdx_seventh_case_vcr = vdx_columns['Video Completions']/vdx_columns['ENGAGEMENTS']
        choice_vdx_eigth_case_vcr = vdx_columns['Video Completions']/vdx_columns['IMPRESSIONS']
        choice_vdx_tenth_case_vcr = vdx_columns['Video Completions']/vdx_columns['DPE100']


        vdx_columns['Video Completion Rate'] = np.select([vdx_value_seventh_case,vdx_value_eight_case,vdx_value_tenth_case,vdx_value_eleventh_case,vdx_value_tweleth_case],
                                                         [choice_vdx_seventh_case_vcr,choice_vdx_eigth_case_vcr,choice_vdx_tenth_case_vcr,
                                                          choice_vdx_seventh_case_vcr,choice_vdx_eigth_case_vcr])


        # final columns

        vdx_by_day_final = vdx_columns.loc[:,['Placement# Name','PRODUCT','DAY','Impressions','Engagements',
                                              'Engagement Rate','Clickthroughs','CTR','Video Completions','Video Completion Rate']]

        vdx_by_day_final.sort_values(['Placement# Name', 'PRODUCT', 'DAY'], ascending=[True, True, True],
                                     inplace=True)

        self.vdx_by_day_final = vdx_by_day_final


    def write_video_data(self):
        """

        Writing Video Data

        """

        unique_plc_summary = self.placement_summary_final['Placement# Name'].nunique()
        self.unique_plc_summary = unique_plc_summary

        try:
            info_client = self.config.client_info.to_excel(self.config.writer, sheet_name="VDX Details",
                                                           startcol=1, startrow=1, index=True, header=False)

            info_campaign = self.config.campaign_info.to_excel(self.config.writer, sheet_name="VDX Details",
                                                               startcol=1, startrow=2, index=True, header=False)

            info_ac_mgr = self.config.ac_mgr.to_excel(self.config.writer, sheet_name="VDX Details", startcol=4,
                                                      startrow=1, index=True, header=False)

            info_sales_rep = self.config.sales_rep.to_excel(self.config.writer, sheet_name="VDX Details",
                                                            startcol=4, startrow=2, index=True, header=False)

            info_campaign_date = self.config.sdate_edate_final.to_excel(self.config.writer,
                                                                        sheet_name="VDX Details", startcol=7,
                                                                        startrow=1, index=True, header=False)

            info_agency = self.config.agency_info.to_excel(self.config.writer, sheet_name="VDX Details",
                                                           startcol=1, startrow=3, index=True, header=False)

            info_currency = self.config.currency_info.to_excel(self.config.writer, sheet_name="VDX Details",
                                                               startcol=7, startrow=3, index=True, header=False)

        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass


        startline_placement = 9
        try:

            if self.sqlscript.read_sql__v_d_x_mv.empty:
                pass
            else:
                for placement, placement_df in self.placement_summary_final.groupby('Placement# Name'):

                    write_pl = placement_df.to_excel(self.config.writer,
                                                     sheet_name="VDX Details".format(self.config.ioid),
                                                     startcol=1, startrow=startline_placement,
                                                     columns=["Placement# Name"],
                                                     header=False, index=False)

                    if placement_df.iloc[0, 0] != "Grand Total":
                        startline_placement += 1

                    write_pls = placement_df.to_excel(self.config.writer,
                                                      sheet_name="VDX Details".format(self.config.ioid),
                                                      startcol=1, startrow=startline_placement, columns=["PRODUCT",
                                                                                                         "IMPRESSIONS",
                                                                                                         "ENGAGEMENTS",
                                                                                                         "Engagements Rate",
                                                                                                         "Clickthroughs",
                                                                                                         "Viewer CTR",
                                                                                                         "Engager CTR",
                                                                                                         "Video Completions",
                                                                                                         "Viewer VCR",
                                                                                                         "Engager VCR",
                                                                                                         "Interaction Rate",
                                                                                                         "Active Time Spent"],
                                                      header=False, index=False)


                    startline_placement += len(placement_df) + 1


        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        startline_adsize = 9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 3
        try:

            if self.sqlscript.read_sql_adsize_km.empty:
                pass
            else:
                for adzise, adsize_df in self.placement_adsize_final.groupby('Placement# Name'):

                    write_adsize_plc = adsize_df.to_excel(self.config.writer,
                                                          sheet_name="VDX Details".format(self.config.ioid),
                                                          startcol=1, startrow=startline_adsize,
                                                          columns=["Placement# Name"],
                                                          header=False, index=False)

                    if adsize_df.iloc[0, 0] != "Grand Total":
                        startline_adsize += 1

                    write_adsize = adsize_df.to_excel(self.config.writer,
                                                      sheet_name="VDX Details".format(self.config.ioid),
                                                      startcol=1, startrow=startline_adsize,
                                                      columns=["ADSIZE", "IMPRESSIONS", "ENGAGEMENTS", "Engagements Rate",
                                                               "Clickthroughs", "Viewer CTR", "Engager CTR",
                                                               "Video Completions", "Viewer VCR", "Engager VCR",
                                                               "Interaction Rate", "Active Time Spent"],
                                                      header=False, index=False)

                    startline_adsize += len(adsize_df) + 1
        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        startline_video = 9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 3 + len(
            self.placement_adsize_final) + self.unique_plc_summary * 2 + 3
        try:

            if self.sqlscript.read_sql_video_km.empty:
                pass
            else:
                for video, video_df in self.placement_by_video_final.groupby('Placement# Name'):
                    write_video_plc = video_df.to_excel(self.config.writer, sheet_name="VDX Details".format(
                        self.config.ioid),
                                                        startcol=1, startrow=startline_video,
                                                        columns=["Placement# Name"],
                                                        header=False, index=False)

                    write_video = video_df.to_excel(self.config.writer,
                                                    sheet_name="VDX Details".format(self.config.ioid),
                                                    startcol=1, startrow=startline_video + 1,
                                                    columns=["PRODUCT", "VIDEONAME",
                                                             "Views", "25_pc_video",
                                                             "50_pc_video",
                                                             "75_pc_video",
                                                             "100_pc_video",
                                                             "Video Completion Rate"],
                                                    header=False, index=False)
                    startline_video += len(video_df) + 2

        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        startline_player = 9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 3 + len(
            self.placement_adsize_final) + self.unique_plc_summary * 2 + 3 + len(
            self.placement_by_video_final) + self.unique_plc_summary * 2 + 2
        try:
            if self.sqlscript.read_sql_video_player_interaction.empty:
                pass
            else:
                write_player_interaction = self.video_player_final.to_excel(self.config.writer,
                                                                            sheet_name="VDX Details".format(
                                                                                self.config.ioid),
                                                                            startcol=1, startrow=startline_player,
                                                                            index=False)
        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.sqlscript.read_sql_ad_intraction.empty:
                pass
            else:
                if self.intractions_clicks_new.empty:
                    pass
                else:
                    write_intraction_clicks = self.intractions_clicks_new.to_excel(self.config.writer,
                                                                                   sheet_name="VDX Details".format(
                                                                                       self.config.ioid),
                                                                                   startcol=9,
                                                                                   startrow=startline_player,
                                                                                   index=False, merge_cells=False)

                if self.intractions_intrac_new.empty:
                    pass
                else:
                    write_intraction = self.intractions_intrac_new.to_excel(self.config.writer,
                                                                            sheet_name="VDX Details".format(
                                                                                self.config.ioid),
                                                                            startcol=9 +
                                                                                     self.intractions_clicks_new.shape[
                                                                                         1],
                                                                            startrow=startline_player, index=False,
                                                                            merge_cells=False)

        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

    def write_by_day_data(self):

        """writing by day data"""
        workbook = self.config.writer.book
        worksheet = self.config.writer.sheets["VDX Details"]
        format_bold_sum = workbook.add_format({"bold": True, "num_format": "#,##0"})
        format_num = workbook.add_format({"num_format": "#,##0"})
        format_percent = workbook.add_format({"num_format": "0.00%"})
        format_bold_row = workbook.add_format({"bold": True})
        format_bold_percent = workbook.add_format({"bold": True, "num_format": "0.00%"})
        unique_product = self.placement_by_video_final['PRODUCT'].nunique()

        self.unique_product = unique_product

        startline_by_day = 9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 3 + \
                           len(self.placement_adsize_final) + self.unique_plc_summary * 2 + 3 + \
                           len(self.placement_by_video_final) + self.unique_plc_summary * 2 + 2 + \
                           len(self.video_player_final) + 7


        if self.sqlscript.read_sql_video_day.empty:
            pass
        else:
            for day_vdx, day_vdx_df in self.vdx_by_day_final.groupby(['Placement# Name']):

                write_vdx_day = day_vdx_df.to_excel(self.config.writer,sheet_name="VDX Details",startcol=1,startrow=startline_by_day,
                                                    index=False,header=False,columns=['Placement# Name'])

                write_vdx_day_new = day_vdx_df.to_excel(self.config.writer,sheet_name="VDX Details",startcol=1,
                                                        startrow=startline_by_day+1,index=False,header=False,
                                                        columns=['PRODUCT','DAY','Impressions','Engagements',
                                                                 'Engagement Rate', 'Clickthroughs', 'CTR',
                                                                 'Video Completions', 'Video Completion Rate'])



                startline_by_day += len(day_vdx_df)+1
                end_row = startline_by_day

                worksheet.write_string(startline_by_day,1,'Subtotal',format_bold_row)
                start_row = startline_by_day - len(day_vdx_df) + 1

                #print(end_row)
                worksheet.write_formula(startline_by_day, 3, '=sum(D{}:D{})'.format(start_row, end_row),format_bold_sum)
                worksheet.write_formula(startline_by_day, 4, '=sum(E{}:E{})'.format(start_row, end_row),format_bold_sum)
                worksheet.write_formula(startline_by_day, 6, '=sum(G{}:G{})'.format(start_row, end_row),format_bold_sum)
                worksheet.write_formula(startline_by_day, 8, '=sum(I{}:I{})'.format(start_row, end_row),format_bold_sum)
                #worksheet.write_formula(startline_by_day, 9, '=sum(J{}:J{})'.format(start_row, end_row),format_bold_sum)
                worksheet.write_formula(startline_by_day,5, '=E{}/D{}'.format(startline_by_day+1,startline_by_day+1),format_bold_percent)
                """worksheet.write_formula(startline_by_day,7, '=G{}/D{}'.format(startline_by_day+1,startline_by_day+1),format_bold_percent)"""
                """worksheet.write_formula(startline_by_day, 9, '=I{}/D{}'.format(startline_by_day+1, startline_by_day+1),
                                        format_bold_percent)"""
                worksheet.conditional_format(start_row-1,3,end_row,4,{"type":"no_blanks","format":format_num})
                worksheet.conditional_format(start_row-1,5,end_row,5,{"type":"no_blanks","format":format_percent})
                worksheet.conditional_format(start_row-1,6,end_row,6,{"type":"no_blanks","format":format_num})
                worksheet.conditional_format(start_row-1,7,end_row,7,{"type":"no_blanks","format":format_percent})
                worksheet.conditional_format(start_row-1,8,end_row,8,{"type":"no_blanks","format":format_num})
                worksheet.conditional_format(start_row-1,9,end_row,9,{"type":"no_blanks","format":format_percent})

                startline_by_day += 2


    def formatting_Video(self):
        """
        Formatting
        """

        try:
            workbook = self.config.writer.book
            worksheet = self.config.writer.sheets["VDX Details".format(self.config.ioid)]
            worksheet.hide_gridlines(2)
            worksheet.set_row(0, 6)
            worksheet.set_column("A:A", 2)
            worksheet.set_zoom(75)
            worksheet.insert_image("O6", "Exponential.png", {"url": "https://www.tribalfusion.com"})
            worksheet.insert_image("O2", "Client_Logo.png")
            worksheet.write_string(2, 8, self.config.status)
            worksheet.write_string(2, 7, "Campaign Status")
            # worksheet.write_string(3, 1, "Agency Name")
            # worksheet.write_string(3, 7, "Currency")

            format_num = workbook.add_format({"num_format": "#,##0"})
            number_cols_plc_summary = self.placement_summary_final.shape[1]
            number_cols_adsize = self.placement_adsize_final.shape[1]
            number_cols_video = self.placement_by_video_final.shape[1]

            format_hearder_right = workbook.add_format({"bold": True, "bg_color": '#00B0F0', "align": "right"})
            format_hearder_left = workbook.add_format({"bold": True, "bg_color": '#00B0F0', "align": "left"})
            format_colour = workbook.add_format({"bg_color": '#00B0F0'})
            format_campaign_info = workbook.add_format({"bold": True, "bg_color": '#00B0F0', "align": "left"})
            format_grand_total = workbook.add_format({"bold": True, "bg_color": "#A5A5A5", "num_format": "#,##0"})
            format_grand = workbook.add_format({"bold": True, "bg_color": "#A5A5A5"})

            worksheet.conditional_format("A1:R5", {"type": "blanks", "format": format_campaign_info})

            worksheet.conditional_format("A1:R5", {"type": "no_blanks", "format": format_campaign_info})

            worksheet.write_string(7, 1, "VDX Performance KPIs - by Placement and Platform",
                                   format_hearder_left)

            worksheet.write_string(8, 1, "Unit", format_hearder_left)
            worksheet.write_string(8, 2, "Impressions", format_hearder_right)
            worksheet.write_string(8, 3, "Engagements", format_hearder_right)
            worksheet.write_string(8, 4, "Engagement Rate", format_hearder_right)
            worksheet.write_string(8, 5, "Clickthroughs", format_hearder_right)
            worksheet.write_string(8, 6, "Viewer CTR", format_hearder_right)
            worksheet.write_string(8, 7, "Engager CTR", format_hearder_right)
            worksheet.write_string(8, 8, "Video Completions", format_hearder_right    )
            worksheet.write_string(8, 9, "Viewer VCR (Primary Video)", format_hearder_right)
            worksheet.write_string(8, 10, "Engager VCR (Primary Video)", format_hearder_right)
            worksheet.write_string(8, 11, "Interaction Rate", format_hearder_right)
            worksheet.write_string(8, 12, "Active Time Spent", format_hearder_right)
            worksheet.conditional_format(7, 1, 7, number_cols_plc_summary - 1,
                                         {"type": "blanks", "format": format_colour})

            percent_fmt = workbook.add_format({"num_format": "0.00%", "align": "right"})

            grand_fmt = workbook.add_format({"num_format": "0.00%", "bg_color": '#A5A5A5', "bold": True})

            ats_format = workbook.add_format({"bg_color": '#A5A5A5', "bold": True})


            ## code added by Gaurav -- start (for Imp, Eng, Vd Completions)

            worksheet.write_string(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1, 1,
                                   "Ad Size Breakdown",
                                   format_hearder_left)
            worksheet.write_string(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 2, 1,
                                   "Ad Size",
                                   format_hearder_left)
            worksheet.write_string(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 2, 2,
                                   "Impressions",
                                   format_hearder_right)
            worksheet.write_string(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 2, 3,
                                   "Engagements",
                                   format_hearder_right)
            worksheet.write_string(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 2, 4,
                                   "Engagement Rate",
                                   format_hearder_right)
            worksheet.write_string(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 2, 5,
                                   "Clickthoughs",
                                   format_hearder_right)
            worksheet.write_string(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 2, 6,
                                   "Viewer CTR",
                                   format_hearder_right)
            worksheet.write_string(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 2, 7,
                                   "Engager CTR",
                                   format_hearder_right)
            worksheet.write_string(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 2, 8,
                                   "Video Completions",
                                   format_hearder_right)
            worksheet.write_string(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 2, 9,
                                   "Viewer VCR (Primary Video)",
                                   format_hearder_right)
            worksheet.write_string(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 2, 10,
                                   "Engager VCR (Primary Video)",
                                   format_hearder_right)
            worksheet.write_string(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 2, 11,
                                   "Interaction Rate",
                                   format_hearder_right)
            worksheet.write_string(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 2, 12,
                                   "Active Time Spent", format_hearder_right)

            ## code added by Gaurav -- end


            worksheet.conditional_format(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1, 1,
                                         9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1,
                                         number_cols_adsize - 1, {
                                             "type": "blanks",
                                             "format": format_colour})

            worksheet.write_string(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                   self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3,
                                   1, "Video Details", format_hearder_left)

            worksheet.write_string(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                   self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 4, 1, "Unit",
                                   format_hearder_left)

            worksheet.write_string(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                   self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 4, 2,
                                   "Video Name",
                                   format_hearder_right)

            worksheet.write_string(9 + self.placement_summary_final.shape[0] +
                                   self.unique_plc_summary * 2 + 1 + self.placement_adsize_final.shape[
                                       0] + self.unique_plc_summary * 2 + 4,
                                   3, "Views", format_hearder_right)

            worksheet.write_string(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                   self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 4, 4,
                                   "25% View",
                                   format_hearder_right)

            worksheet.write_string(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                   self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 4, 5,
                                   "50% View",
                                   format_hearder_right)

            worksheet.write_string(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                   self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 4, 6,
                                   "75% View",
                                   format_hearder_right)

            worksheet.write_string(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                   self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 4, 7,
                                   "Video Completion",
                                   format_hearder_right)

            worksheet.write_string(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                   self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 4, 8,
                                   "Video Completion Rate",
                                   format_hearder_right)

            worksheet.conditional_format(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                         self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3, 1,
                                         9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                         self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3,
                                         number_cols_video - 1,
                                         {"type": "blanks", "format": format_colour})

            # Writing Intractions table information
            worksheet.write_string(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                   self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 +
                                   self.placement_by_video_final.shape[0] + self.unique_plc_summary * 2 + 2,
                                   1, "Interaction Details",
                                   format_hearder_left)

            # Interaction Mute Unmute Table blank row formatting on second header
            worksheet.conditional_format(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                         self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 +
                                         self.placement_by_video_final.shape[0] + self.unique_plc_summary * 2 + 3, 1,
                                         9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                         self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 +
                                         self.placement_by_video_final.shape[0] + self.unique_plc_summary * 2 + 3,
                                         9 + self.intractions_clicks_new.shape[1] + self.intractions_intrac_new.shape[
                                             1],
                                         {"type": "blanks", "format": format_colour})

            worksheet.write_string(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                   self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 +
                                   self.placement_by_video_final.shape[0] + self.unique_plc_summary * 2 + 3, 2,
                                   "Video Player Interactions",
                                   format_hearder_right)

            # Colour Formatting for Interaction Table
            #print(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1)
            #print(self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 3)
            #print(self.placement_by_video_final.shape[0] + self.unique_plc_summary * 2 + 2)

            start_row_intraction = 9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 + \
                                   self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 + \
                                   self.placement_by_video_final.shape[0] + self.unique_plc_summary * 2 + 2

            worksheet.conditional_format(start_row_intraction, 2,
                                         start_row_intraction,
                                         9 + self.intractions_clicks_new.shape[1] + self.intractions_intrac_new.shape[
                                             1],
                                         {
                                             "type": "blanks",
                                             "format": format_colour
                                         })

            if self.intractions_clicks_new.empty:
                pass
            else:
                worksheet.write_string(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                       self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 +
                                       self.placement_by_video_final.shape[0] + self.unique_plc_summary * 2 + 3, 9,
                                       "Clickthroughs", format_hearder_right)

            if self.intractions_intrac_new.empty:
                pass
            else:
                worksheet.write_string(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                       self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 +
                                       self.placement_by_video_final.shape[0] + self.unique_plc_summary * 2 + 3,
                                       9 + self.intractions_clicks_new.shape[1], "Ad Interactions",
                                       format_hearder_right)

            worksheet.write_string(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                   self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 +
                                   self.placement_by_video_final.shape[0] + self.unique_plc_summary * 2 + 4,
                                   9 + self.intractions_clicks_new.shape[1] + self.intractions_intrac_new.shape[1],
                                   "Total Interactions", format_hearder_right)

            worksheet.write_string(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                   self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 +
                                   self.placement_by_video_final.shape[0] + self.unique_plc_summary * 2 + 4 +
                                   self.video_player_final.shape[0] + 1,
                                   1, "Grand Total", format_grand)

            # Sum for Mute Unmute Table
            for col in range(2, 9 + self.intractions_clicks_new.shape[1] + self.intractions_intrac_new.shape[1] + 1):
                cell_location = xl_rowcol_to_cell(
                    9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                    self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 +
                    self.placement_by_video_final.shape[0] + self.unique_plc_summary * 2 + 4 +
                    self.video_player_final.shape[0] + 1
                    , col)
                start_range = xl_rowcol_to_cell(
                    9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                    self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 +
                    self.placement_by_video_final.shape[0] + self.unique_plc_summary * 2 + 5,
                    col)

                end_range = xl_rowcol_to_cell(
                    9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                    self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 +
                    self.placement_by_video_final.shape[0] + self.unique_plc_summary * 2 + 4 +
                    self.video_player_final.shape[0], col)

                formula = '=sum({:s}:{:s})'.format(start_range, end_range)

                worksheet.write_formula(cell_location, formula, format_grand_total)

            # Sum of total Interaction by row
            start_range_x = 9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 + \
                            self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 + \
                            self.placement_by_video_final.shape[0] + self.unique_plc_summary * 2 + 5

            for row in range(self.video_player_final.shape[0]):
                cell_range = xl_range(start_range_x, 2, start_range_x,
                                      9 + self.intractions_clicks_new.shape[1] + self.intractions_intrac_new.shape[
                                          1] - 1)
                formula = 'sum({:s})'.format(cell_range)
                worksheet.write_formula(start_range_x,
                                        9 + self.intractions_clicks_new.shape[1] + self.intractions_intrac_new.shape[1],
                                        formula, format_num)
                start_range_x += 1

            # Header Formatting for Mute Unmute Table
            worksheet.conditional_format(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                         self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 +
                                         self.placement_by_video_final.shape[0] + self.unique_plc_summary * 2 + 4, 1,
                                         9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                         self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 +
                                         self.placement_by_video_final.shape[0] + self.unique_plc_summary * 2 + 4,
                                         9 + self.intractions_clicks_new.shape[1] + self.intractions_intrac_new.shape[
                                             1], {"type": "no_blanks", "format": format_hearder_left})

            worksheet.conditional_format(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                         self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 +
                                         self.placement_by_video_final.shape[0] + self.unique_plc_summary * 2 + 4, 1,
                                         9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                         self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 +
                                         self.placement_by_video_final.shape[0] + self.unique_plc_summary * 2 + 4,
                                         9 + self.intractions_clicks_new.shape[1] + self.intractions_intrac_new.shape[
                                             1], {"type": "blanks", "format": format_hearder_left})

            """
            worksheet.conditional_format(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4, 1,
                                         9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4,
                                         number_cols_plc_summary - 2, {"type": "blanks", "format": grand_fmt})

            worksheet.conditional_format(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4, 1,
                                         9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4,
                                         number_cols_plc_summary - 2, {"type": "no_blanks", "format": grand_fmt})
            """

            ## added by Gaurav - grand total formatting for summary table -- start

            worksheet.conditional_format(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4,
                                         1, 9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4,
                                         3, {"type": "no_blanks", "format": format_grand_total})
            worksheet.conditional_format(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4,
                                         5, 9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4,
                                         5, {"type": "no_blanks", "format": format_grand_total})
            worksheet.conditional_format(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4,
                                         8, 9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4,
                                         8, {"type": "no_blanks", "format": format_grand_total})
            worksheet.conditional_format(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4,
                                         12, 9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4,
                                         12, {"type": "no_blanks", "format": ats_format})

            worksheet.conditional_format(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4,
                                         4, 9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4,
                                         4, {"type": "no_blanks", "format": grand_fmt})
            worksheet.conditional_format(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4,
                                         6, 9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4,
                                         7, {"type": "no_blanks", "format": grand_fmt})
            worksheet.conditional_format(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4,
                                         11, 9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4,
                                         11, {"type": "no_blanks", "format": grand_fmt})

            worksheet.conditional_format(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4,
                                         9, 9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 4,
                                         10, {"type": "blanks", "format": grand_fmt})


            # ad-size grand total formatting -- Gaurav
            startRow = 9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 + \
                       self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 - 4

            worksheet.conditional_format(startRow, 1, startRow, 3,
                                         {"type": "no_blanks", "format": format_grand_total})
            worksheet.conditional_format(startRow, 5, startRow, 5,
                                         {"type": "no_blanks", "format": format_grand_total})
            worksheet.conditional_format(startRow, 8, startRow, 8,
                                         {"type": "no_blanks", "format": format_grand_total})
            worksheet.conditional_format(startRow, 12, startRow, 12,
                                         {"type": "no_blanks", "format": ats_format})

            worksheet.conditional_format(startRow, 4, startRow, 4,
                                         {"type": "no_blanks", "format": grand_fmt})
            worksheet.conditional_format(startRow, 6, startRow, 7,
                                         {"type": "no_blanks", "format": grand_fmt})
            worksheet.conditional_format(startRow, 11, startRow, 11,
                                         {"type": "no_blanks", "format": grand_fmt})

            worksheet.conditional_format(startRow, 9, startRow, 10,
                                         {"type": "blanks", "format": grand_fmt})

            ##
            """
            worksheet.conditional_format(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                         self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 - 4, 1,
                                         9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                         self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 - 4,
                                         number_cols_adsize - 2,
                                         {"type": "blanks", "format": grand_fmt})

            worksheet.conditional_format(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                         self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 - 4, 1,
                                         9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                         self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 - 4,
                                         number_cols_adsize - 2, {"type": "no_blanks", "format": grand_fmt})

            worksheet.conditional_format(9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                         self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 - 4,
                                         number_cols_adsize - 1,
                                         9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 +
                                         self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 - 4,
                                         number_cols_adsize - 1, {"type": "no_blanks", "format": ats_format})

            """
            ## added by Gaurav -- end



            ## Format table contents here

            # format summary table
            """
            for col in range(2, number_cols_plc_summary - 1):
                start_plc_row = 10
                end_plc_row = 9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 6
                worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                             {"type": "no_blanks", "format": format_num})
            """

            ## added by Gaurav - start
            for col in range(2, number_cols_plc_summary - 1):
                start_plc_row = 10
                end_plc_row = 9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 1 - 6

                if col == 2 or col == 3 or col == 5 or col == 8:
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": format_num})
                else:
                    worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                                 {"type": "no_blanks", "format": percent_fmt})



            for col in range(2, number_cols_adsize - 1):
                start_adsize_row = 9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 4
                end_adsize_row = 9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 + \
                                 self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 - 6

                if col == 2 or col == 3 or col == 5 or col == 8:
                    worksheet.conditional_format(start_adsize_row, col, end_adsize_row, col,
                                                 {"type": "no_blanks", "format": format_num})
                else:
                    worksheet.conditional_format(start_adsize_row, col, end_adsize_row, col,
                                                 {"type": "no_blanks", "format": percent_fmt})

            ## added by Gaurav - end



            for col in range(3, number_cols_video - 1):
                start_video_row = 9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 3 + len(
                    self.placement_adsize_final) + self.unique_plc_summary * 2 + 4
                end_video_row = 9 + self.placement_summary_final.shape[0] + \
                                self.unique_plc_summary * 2 + 1 + self.placement_adsize_final.shape[0] + \
                                self.unique_plc_summary * 2 + 3 + self.placement_by_video_final.shape[0] + \
                                self.unique_plc_summary * 2 + 3 - 4
                worksheet.conditional_format(start_video_row, col, end_video_row, col,
                                             {"type": "no_blanks", "format": format_num})

            for col in range(number_cols_video - 1, number_cols_video):
                start_video_row_vcr = 9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 3 + len(
                    self.placement_adsize_final) + self.unique_plc_summary * 2 + 4
                end_video_row_vcr = 9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 + \
                                    self.placement_adsize_final.shape[0] + \
                                    self.unique_plc_summary * 2 + 3 + self.placement_by_video_final.shape[0] + \
                                    self.unique_plc_summary * 2 + 3 - 4
                worksheet.conditional_format(start_video_row_vcr, col, end_video_row_vcr, col,
                                             {"type": "no_blanks", "format": percent_fmt})

            for col in range(2, 9 + self.intractions_clicks_new.shape[1] + self.intractions_intrac_new.shape[1]):
                start_intraction_row = 9 + self.placement_summary_final.shape[0] + \
                                       self.unique_plc_summary * 2 + 1 + self.placement_adsize_final.shape[0] + \
                                       self.unique_plc_summary * 2 + 3 + self.placement_by_video_final.shape[
                                           0] + self.unique_plc_summary * 2 + 6

                end_intraction_row = 9 + self.placement_summary_final.shape[0] + self.unique_plc_summary * 2 + 1 + \
                                     self.placement_adsize_final.shape[0] + self.unique_plc_summary * 2 + 3 + \
                                     self.placement_by_video_final.shape[0] + \
                                     self.unique_plc_summary * 2 + 5 + self.video_player_final.shape[0]

                worksheet.conditional_format(start_intraction_row, col, end_intraction_row, col,
                                             {"type": "no_blanks", "format": format_num})

            alignment = workbook.add_format({"align": "right"})
            alignment_left = workbook.add_format({"align": "left"})

            if self.intractions_clicks_new.empty or self.intractions_intrac_new.empty:
                worksheet.set_column("C:R", 25, alignment)
            else:
                worksheet.set_column(2, 9 + self.intractions_clicks_new.shape[1] + self.intractions_intrac_new.shape[1],
                                     25,
                                     alignment)

            worksheet.set_column("B:B", 47)
            worksheet.set_row(1, None, alignment_left)
            worksheet.set_row(2, None, alignment_left)
            worksheet.set_row(3, None, alignment_left)

        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

    def format_by_day(self):
        """formatting a day wise table"""

        workbook = self.config.writer.book
        worksheet = self.config.writer.sheets["VDX Details"]
        row_start = 9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 3 + \
                    len(self.placement_adsize_final) + self.unique_plc_summary * 2 + 3 + \
                    len(self.placement_by_video_final) + self.unique_plc_summary * 2 + 2 + \
                    len(self.video_player_final) + 6

        # print(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 3)
        # print(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 3 + len(self.placement_adsize_final) + self.unique_plc_summary * 2 + 3)
        # print(9 + len(self.placement_summary_final) + self.unique_plc_summary * 2 + 3 + len(self.placement_adsize_final) + self.unique_plc_summary * 2 + 3 + len(self.placement_by_video_final) + self.unique_plc_summary * 2 + 3)

        unique_plc = self.placement_by_video_final['Placement# Name'].nunique()
        self.unique_plc = unique_plc
        grand_row = row_start + 1 + len(self.vdx_by_day_final) + (self.unique_plc * 2) + self.unique_plc

        format_grand_row = workbook.add_format({"bold": True, "bg_color": "#A5A5A5"})
        format_grand_num = workbook.add_format({"num_format": "#,##0", "bold": True, "bg_color": "#A5A5A5"})
        format_grand_percent = workbook.add_format(
            {"num_format": "0.00%", "bold": True, "bg_color": "#A5A5A5", "align": "right"})
        format_header = workbook.add_format({"bold": True, "bg_color": '#00B0F0'})
        format_header_align = workbook.add_format({"bold": True, "bg_color": '#00B0F0', "align": "right"})
        format_colour = workbook.add_format({"bg_color": '#00B0F0'})
        format_grand_colour = workbook.add_format({"bg_color": "#A5A5A5"})

        worksheet.write_string(row_start, 1, "Unit", format_header)
        worksheet.write_string(row_start, 2, "Date", format_header_align)
        worksheet.write_string(row_start, 3, "Impressions", format_header_align)
        worksheet.write_string(row_start, 4, "Engagements", format_header_align)
        worksheet.write_string(row_start, 5, "Engagement Rate", format_header_align)
        worksheet.write_string(row_start, 6, "Clickthroughs", format_header_align)
        worksheet.write_string(row_start, 7, "CTR", format_header_align)
        worksheet.write_string(row_start, 8, "Video Completions", format_header_align)
        worksheet.write_string(row_start, 9, "Video Completion Rate", format_header_align)
        worksheet.write_string(row_start - 1, 1, "Daily Breakdown", format_header)
        worksheet.conditional_format(row_start - 1, 1, row_start - 1, 9, {"type": "blanks", "format": format_colour})
        worksheet.conditional_format(grand_row, 2, grand_row, 2, {"type": "blanks", "format": format_grand_colour})
        worksheet.write_string(grand_row, 1, "Grand Total", format_grand_row)
        worksheet.write_formula(grand_row, 3,
                                '=SUMIFS(D{}:D{},B{}:B{},"Subtotal")'.format(row_start + 3, grand_row - 1, row_start + 3,
                                                                             grand_row - 1), format_grand_num)
        worksheet.write_formula(grand_row, 4, '=SUMIFS(E{}:E{},B{}:B{},"Subtotal")'.format(row_start + 3, grand_row - 1,
                                                                                           row_start + 3,
                                                                                           grand_row - 1),
                                format_grand_num)

        worksheet.write_formula(grand_row, 5, '=E{}/D{}'.format(grand_row + 1, grand_row + 1),
                                format_grand_percent)

        worksheet.write_formula(grand_row, 6, '=SUMIFS(G{}:G{},B{}:B{},"Subtotal")'.format(row_start + 3, grand_row - 1,
                                                                                           row_start + 3,
                                                                                           grand_row - 1),
                                format_grand_num)

        """worksheet.write_formula(grand_row, 7, '=G{}/E{}'.format(grand_row + 1, grand_row + 1),
                                format_grand_percent)"""

        worksheet.write_formula(grand_row, 8, '=SUMIFS(I{}:I{},B{}:B{},"Subtotal")'.format(row_start + 3, grand_row - 1,
                                                                                           row_start + 3,
                                                                                           grand_row - 1),
                                format_grand_num)

        """worksheet.write_formula(grand_row, 9, '=I{}/D{}'.format(grand_row + 1, grand_row + 1),
                                format_grand_percent)"""

        worksheet.conditional_format(grand_row,7,grand_row,7,{"type":"blanks","format":format_grand_colour})
        worksheet.conditional_format(grand_row,9,grand_row,9,{"type":"blanks","format":format_grand_colour})


    def main(self):
        """
    Main Function
        """
        self.config.common_columns_summary()
        self.config.logger.info("Start Creating VDX Details Sheet for IO - {} ".format(self.config.ioid) + " at " + str(
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        if self.sqlscript.read_sql__v_d_x.empty:
            self.logger.info("No live VDX placements for IO - {}".format(self.config.ioid))
            pass
        else:
            self.logger.info("Live VDX placements found for IO - {}".format(self.config.ioid) + " at " + str(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
            self.access_vdx_placement_columns()
            self.access_vdx_adsize_columns()
            self.vdx_video_details()
            self.vdx_player_interaction()
            self.vdx_ad_interaction()
            self.vdx_day_data()
            self.write_video_data()
            self.write_by_day_data()
            self.formatting_Video()
            self.format_by_day()
            self.logger.info("VDX Details Sheet Created for IO - {}".format(self.config.ioid) + " at " + str(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))


if __name__ == "__main__":
    pass
    # enable it when running for individual file
    # c = config.Config('Origin', 608607,'2018-04-16','2018-04-23')
    # o = Video (c)
    # o.main ()
    # c.saveAndCloseWriter ()
