# coding=utf-8
# !/usr/bin/env python
"""
Created by:Dharmendra
Date:2018-03-23
"""
import datetime
import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell, xl_range
import pandas.io.formats.excel
from functools import reduce
from SQLScript import SqlScript

pandas.io.formats.excel.header_style = None


class Intraction(object):
    """
Preroll placements Class
    """

    def __init__(self, config,sqlscript):

        #super(Intraction,self).__init__(self)

        self.config = config
        self.sqlscript = sqlscript
        self.logger = self.config.logger
        self.prerollsummaryfinal = None
        self.videoprerollsummarymvfinal = None
        self.video_player_summary_final = None
        self.intraction_final = None
        self.dayprerollsummaryfinal = None


    def accessing_preroll_columns(self):
        """
        Accessing Columns from Query
        """
        prerollsummaryfinal = None
        videoprerollsummarymvfinal = None
        intraction_final = None
        dayprerollsummaryfinal = None
        video_player_summary_final = None

        try:
            if self.sqlscript.read_sql_preroll_mv.empty:
                pass
            else:
                placementprerollmv = self.sqlscript.read_sql_preroll.merge(self.sqlscript.read_sql_preroll_mv, on="PLACEMENT#")

                prerollsummary = placementprerollmv.loc[:,
                                 ["IO_ID", "PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE", "NET_UNIT_COST",
                                  "GROSS_UNIT_COST", "IMPRESSION",
                                  "CLICKTHROUGHS", "COMPLETIONS", "VIDEO_COMPLETIONS"]]

                preroll_table_exchange = [prerollsummary, self.config.cdb_io_exchange]

                prerollsummarymv = reduce(lambda left, right: pd.merge(left, right, on='IO_ID'), preroll_table_exchange)

                mask_preroll_unit_au_nz_gb_not_null = (prerollsummarymv["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (prerollsummarymv["GROSS_UNIT_COST"]!=0)#.notnull())
                choices_preroll_unit_au_nz_gb_not_null = prerollsummarymv["GROSS_UNIT_COST"] * prerollsummarymv["Currency Exchange Rate"]

                mask_preroll_unit_au_nz_gb_is_null = (prerollsummarymv["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (prerollsummarymv["GROSS_UNIT_COST"]==0)#.isnull())
                choices_preroll_unit_au_nz_gb_is_null = prerollsummarymv["NET_UNIT_COST"] * prerollsummarymv["Currency Exchange Rate"]


                mask_preroll_unit_other_not_null = (~prerollsummarymv["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (prerollsummarymv["NET_UNIT_COST"]!=0)#.notnull())
                choices_preroll_unit_other_not_null = prerollsummarymv["NET_UNIT_COST"] * prerollsummarymv["Currency Exchange Rate"]

                mask_preroll_unit_other_is_null = (~prerollsummarymv["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (prerollsummarymv["NET_UNIT_COST"]==0)#.isnull())
                choices_preroll_unit_other_is_null = prerollsummarymv["GROSS_UNIT_COST"] * prerollsummarymv["Currency Exchange Rate"]


                prerollsummarymv["UNIT_COST"] = np.select([mask_preroll_unit_au_nz_gb_not_null,
                                                           mask_preroll_unit_au_nz_gb_is_null,
                                                           mask_preroll_unit_other_not_null,
                                                           mask_preroll_unit_other_is_null],
                                                          [choices_preroll_unit_au_nz_gb_not_null,
                                                           choices_preroll_unit_au_nz_gb_is_null,
                                                           choices_preroll_unit_other_not_null,
                                                           choices_preroll_unit_other_is_null],default=0.00)

                mask1 = prerollsummarymv["COST_TYPE"].isin(['CPM'])
                choiceprerollsummarymvcpm = prerollsummarymv["VIDEO_COMPLETIONS"]
                mask2 = prerollsummarymv["COST_TYPE"].isin(['CPCV'])
                choiceprerollsummarymvcpcv = prerollsummarymv["COMPLETIONS"]

                prerollsummarymv["Video Completions"] = np.select([mask1, mask2], [choiceprerollsummarymvcpm,
                                                                                   choiceprerollsummarymvcpcv])

                choicespendcpm = prerollsummarymv["IMPRESSION"] / 1000 * prerollsummarymv["UNIT_COST"]
                choicespendcpcv = prerollsummarymv["Video Completions"] * prerollsummarymv["UNIT_COST"]

                prerollsummarymv["Spend"] = np.select([mask1, mask2], [choicespendcpm, choicespendcpcv])

                prerollsummary = prerollsummarymv.loc[:, ["PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE", "UNIT_COST"
                    , "IMPRESSION", "CLICKTHROUGHS", "Video Completions", "Spend"]]

                prerollsummary["Placement# Name"] = prerollsummary[["PLACEMENT#", "PLACEMENT_NAME"]].apply(
                    lambda x: ".".join(x), axis=1)

                prerollsummary["CTR"] = prerollsummary["CLICKTHROUGHS"] / prerollsummary["IMPRESSION"]
                prerollsummary["Video Completion Rate"] = prerollsummary["Video Completions"] / prerollsummary["IMPRESSION"]

                prerollsummaryfinal = prerollsummary.loc[:, ["Placement# Name", "COST_TYPE", "UNIT_COST", "IMPRESSION",
                                                             "CLICKTHROUGHS", "CTR", "Video Completions",
                                                             "Video Completion Rate","Spend"]]

        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.sqlscript.read_sql_preroll_video.empty:
                pass
            else:
                videoprerollmv = self.sqlscript.read_sql_preroll.merge(self.sqlscript.read_sql_preroll_video, on="PLACEMENT#")

                videoprerollsummarymv = videoprerollmv.loc[:,
                                        ["PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE", "IMPRESSION", "COMPLETIONS"]]

                placement_by_preroll_video = [videoprerollsummarymv, self.sqlscript.read_sql_video_details]
                preroll_video_summary = reduce(lambda left, right: pd.merge(left, right, on=['PLACEMENT#']),
                                               placement_by_preroll_video)

                mask3 = preroll_video_summary["COST_TYPE"].isin(['CPM'])
                choicevideoprerollcpm = preroll_video_summary["VIDEO_COMPLETIONS"]
                mask4 = preroll_video_summary["COST_TYPE"].isin(['CPCV'])
                choicevideoprerollcpcv = preroll_video_summary["COMPLETIONS"]

                preroll_video_summary["Video Completions"] = np.select([mask3, mask4], [choicevideoprerollcpm,
                                                                                        choicevideoprerollcpcv])
                mask8 = preroll_video_summary["COST_TYPE"].isin(['CPM', 'CPCV'])

                preroll_video_summary_new = preroll_video_summary.loc[
                    preroll_video_summary.reset_index().groupby(['PLACEMENT#'])['VIEWS0'].idxmax()]

                preroll_video_summary_new.loc[mask8, 'VIEWS0'] = preroll_video_summary_new['IMPRESSION']

                preroll_video_summary = preroll_video_summary.drop(preroll_video_summary_new.index).append(
                    preroll_video_summary_new).sort_index()

                preroll_video_summary["Views"] = preroll_video_summary["VIEWS0"]

                preroll_video_summary["Video Completion Rate"] = preroll_video_summary["Video Completions"] / \
                                                                 preroll_video_summary["Views"]

                preroll_video_summary["Placement# Name"] = preroll_video_summary[
                    ["PLACEMENT#", "PLACEMENT_NAME"]].apply(lambda x: ".".join(x), axis=1)

                videoprerollsummarymvfinal = preroll_video_summary.loc[:,
                                             ["Placement# Name", "FEV_INT_VIDEO_DESC", "Views", "VIEWS25", "VIEWS50"
                                                 , "VIEWS75", "Video Completions", "Video Completion Rate"]]

                videoprerollsummarymvfinal = videoprerollsummarymvfinal.loc[:,
                                             ["Placement# Name", "FEV_INT_VIDEO_DESC", "Views", "VIEWS25", "VIEWS50",
                                              "VIEWS75", "Video Completions",
                                              "Video Completion Rate"]]

        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.sqlscript.read_sql_preroll_day.empty:
                pass
            else:
                dayprerollmv = self.sqlscript.read_sql_preroll.merge(self.sqlscript.read_sql_preroll_day, on="PLACEMENT#")

                dayprerollsummary = dayprerollmv.loc[:,
                                    ["IO_ID", "PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE", "NET_UNIT_COST",
                                     "GROSS_UNIT_COST", "DAY_DESC",
                                     "IMPRESSION", "CLICKTHROUGHS", "VIDEO_COMPLETIONS",
                                     "COMPLETIONS"]]

                day_pre_roll_exchange = [dayprerollsummary, self.config.cdb_io_exchange]

                day_pre_roll = reduce(lambda left, right: pd.merge(left, right, on='IO_ID'), day_pre_roll_exchange)

                mask_preroll_unit_au_nz_gb_not_null = (day_pre_roll["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (day_pre_roll["GROSS_UNIT_COST"]!=0)#.notnull())
                choices_preroll_unit_au_nz_gb_not_null = day_pre_roll["GROSS_UNIT_COST"] * day_pre_roll["Currency Exchange Rate"]

                mask_preroll_unit_au_nz_gb_is_null = (day_pre_roll["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (day_pre_roll["GROSS_UNIT_COST"]==0)#.isnull())
                choices_preroll_unit_au_nz_gb_is_null = day_pre_roll["NET_UNIT_COST"] * day_pre_roll["Currency Exchange Rate"]

                mask_preroll_unit_other_not_null = (~day_pre_roll["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (day_pre_roll["NET_UNIT_COST"]!=0)#.notnull())
                choices_preroll_unit_other_not_null = day_pre_roll["NET_UNIT_COST"] * day_pre_roll["Currency Exchange Rate"]

                mask_preroll_unit_other_is_null = (~day_pre_roll["Currency Type"].isin(['AUD', 'NZD', 'GBP'])) & (day_pre_roll["NET_UNIT_COST"]==0)#.isnull())
                choices_preroll_unit_other_is_null = day_pre_roll["GROSS_UNIT_COST"] * day_pre_roll["Currency Exchange Rate"]

                day_pre_roll["UNIT_COST"] = np.select([mask_preroll_unit_au_nz_gb_not_null,
                                                       mask_preroll_unit_au_nz_gb_is_null,
                                                       mask_preroll_unit_other_not_null,
                                                       mask_preroll_unit_other_is_null],
                                                      [choices_preroll_unit_au_nz_gb_not_null,
                                                       choices_preroll_unit_au_nz_gb_is_null,
                                                       choices_preroll_unit_other_not_null,
                                                       choices_preroll_unit_other_is_null],default=0.00)

                dayprerollsummarymv = day_pre_roll.loc[:, ["PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE", "UNIT_COST",
                                                           "DAY_DESC", "IMPRESSION", "CLICKTHROUGHS",
                                                           "VIDEO_COMPLETIONS",
                                                           "COMPLETIONS"]]

                mask5 = dayprerollsummarymv["COST_TYPE"].isin(["CPM"])
                choicedayprerollcpm = dayprerollsummarymv["VIDEO_COMPLETIONS"]
                mask6 = dayprerollsummarymv["COST_TYPE"].isin(["CPCV"])
                choicedayprerollcpcv = dayprerollsummarymv["COMPLETIONS"]

                dayprerollsummarymv["Video Completions"] = np.select([mask5, mask6],
                                                                     [choicedayprerollcpm, choicedayprerollcpcv])
                choicedayspendcpm = dayprerollsummarymv["IMPRESSION"] / 1000 * dayprerollsummarymv["UNIT_COST"]
                choicedayspendcpcv = dayprerollsummarymv["Video Completions"] * dayprerollsummarymv["UNIT_COST"]
                dayprerollsummarymv["Spend"] = np.select([mask5, mask6], [choicedayspendcpm, choicedayspendcpcv])
                dayprerollsummarymv["VCR%"] = dayprerollsummarymv["Video Completions"] / dayprerollsummarymv[
                    "IMPRESSION"]
                dayprerollsummarymv["CTR"] = dayprerollsummarymv["CLICKTHROUGHS"] / dayprerollsummarymv["IMPRESSION"]

                dayprerollsummarymv["Placement# Name"] = dayprerollsummarymv[["PLACEMENT#", "PLACEMENT_NAME"]].apply(
                    lambda x: ".".join(x), axis=1)

                dayprerollsummaryfinal = dayprerollsummarymv.loc[:,
                                         ["Placement# Name", "DAY_DESC", "IMPRESSION", "CLICKTHROUGHS", "CTR",
                                          "Video Completions", "VCR%", "Spend"]]

        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.sqlscript.read_sql_preroll_video_player.empty:
                pass
            else:

                video_player_mv = [self.sqlscript.read_sql_preroll, self.sqlscript.read_sql_preroll_video_player]
                video_player_summary = reduce(lambda left, right: pd.merge(left, right, on=['PLACEMENT#']),
                                              video_player_mv)

                video_player_summary["Placement# Name"] = video_player_summary[["PLACEMENT#", "PLACEMENT_NAME"]].apply(
                    lambda x: ".".join(x), axis=1)
                video_player_summary_final = video_player_summary.loc[:,
                                             ["Placement# Name", "VWRMUTE", "VWRUNMUTE", "VWRPAUSE",
                                              "VWRREWIND", "VWRRESUME", "VWRREPLAY", "VWRFULLSCREEN"]]

        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.sqlscript.read_sql_preroll_interaction.empty:
                pass
            else:

                intractionsummarymv = self.sqlscript.read_sql_preroll.merge(self.sqlscript.read_sql_preroll_interaction,
                                                                            on="PLACEMENT#")

                intractionclick = intractionsummarymv.loc[:,
                                  ["PLACEMENT#", "PLACEMENT_NAME", "CLICK_TAG", "VWR_CLICKTHROUGH"]]
                intractionclick["Placement# Name"] = intractionclick[["PLACEMENT#", "PLACEMENT_NAME"]].apply(
                    lambda x: ".".join(x), axis=1)

                intraction_final = None
                intraction_table_clicks = pd.pivot_table(intractionclick, index='Placement# Name',
                                                         values='VWR_CLICKTHROUGH',
                                                         columns='CLICK_TAG', aggfunc=np.sum, fill_value=0)
                intraction_table_clicks_new = intraction_table_clicks.reset_index()
                intraction_table_clicks_r = intraction_table_clicks_new.loc[:, :]

                intraction_click_table_new = intraction_table_clicks_r.merge(prerollsummaryfinal, on="Placement# Name",
                                                                             how="inner")

                cols_drop = ["COST_TYPE", "UNIT_COST", "IMPRESSION", "CLICKTHROUGHS", "Video Completions",
                             "Video Completion Rate",
                             "CTR", "Spend", "Placement# Name"]
                intraction_new_cols = intraction_click_table_new.drop(cols_drop, axis=1)

                intraction_final = intraction_new_cols.loc[:, :]

        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        self.prerollsummaryfinal = prerollsummaryfinal
        self.videoprerollsummarymvfinal = videoprerollsummarymvfinal
        self.video_player_summary_final = video_player_summary_final
        self.intraction_final = intraction_final
        self.dayprerollsummaryfinal = dayprerollsummaryfinal

    def renameIntraction(self):
        """Renaming Columns"""

        try:
            if self.sqlscript.read_sql_preroll_mv.empty:
                pass
            else:
                rename_preroll_summary_final = self.prerollsummaryfinal.rename(
                    columns={"COST_TYPE": "Cost Type", "UNIT_COST": "Unit Cost",
                             "IMPRESSION": "Impressions",
                             "CLICKTHROUGHS": "Clickthroughs", "CTR": "CTR %",
                             "Video Completion Rate": "VCR %"}, inplace=True)
        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.sqlscript.read_sql_preroll_video.empty:
                pass
            else:
                rename_video_preroll_summary_mv_final = self.videoprerollsummarymvfinal.rename(
                    columns={"FEV_INT_VIDEO_DESC": "Video Name",
                             "VIEWS25": "25% View",
                             "VIEWS50": "50% View",
                             "VIEWS75": "75% View"}, inplace=True)
        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.sqlscript.read_sql_preroll_day.empty:
                pass
            else:
                renameday_preroll_summary_final = self.dayprerollsummaryfinal.rename(
                    columns={"DAY_DESC": "Date", "IMPRESSION": "Impressions",
                             "CLICKTHROUGHS": "Clickthroughs"}, inplace=True)
        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.sqlscript.read_sql_preroll_interaction.empty:
                pass
            else:
                rename_video_player_summary_final = self.video_player_summary_final.rename(columns={"VWRMUTE": "Mute",
                                                                                                    "VWRUNMUTE": "Unmute",
                                                                                                    "VWRPAUSE": "Pause",
                                                                                                    "VWRREWIND": "Rewind",
                                                                                                    "VWRRESUME": "Resume",
                                                                                                    "VWRREPLAY": "Replay",
                                                                                                    "VWRFULLSCREEN": "Fullscreen"},
                                                                                           inplace=True)
        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

    def writePreroll(self):
        """
            writing to excel all data
        """

        try:
            info_client = self.config.client_info.to_excel(self.config.writer, sheet_name="Standard Pre Roll Details",
                                                           startcol=1, startrow=1, index=True, header=False)
            info_campaign = self.config.campaign_info.to_excel(self.config.writer,
                                                               sheet_name="Standard Pre Roll Details",
                                                               startcol=1, startrow=2, index=True, header=False)
            info_ac_mgr = self.config.ac_mgr.to_excel(self.config.writer, sheet_name="Standard Pre Roll Details",
                                                      startcol=4,
                                                      startrow=1, index=True, header=False)
            info_sales_rep = self.config.sales_rep.to_excel(self.config.writer, sheet_name="Standard Pre Roll Details",
                                                            startcol=4, startrow=2, index=True, header=False)
            info_campaign_date = self.config.sdate_edate_final.to_excel(self.config.writer,
                                                                        sheet_name="Standard Pre Roll Details",
                                                                        startcol=7,
                                                                        startrow=1, index=True, header=False)

            info_agency = self.config.agency_info.to_excel(self.config.writer, sheet_name="Standard Pre Roll Details",
                                                           startcol=1, startrow=3, index=True, header=False)
            info_currency = self.config.currency_info.to_excel(self.config.writer,
                                                               sheet_name="Standard Pre Roll Details",
                                                               startcol=7, startrow=3, index=True, header=False)

        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        self.logger.info('Writing placement information on preroll for IO - {}'.format(self.config.ioid))

        try:
            if self.sqlscript.read_sql_preroll_mv.empty:
                pass
            else:
                writing_preroll_summary_final = self.prerollsummaryfinal.to_excel(self.config.writer,
                                                                                  sheet_name="Standard Pre Roll Details".format(
                                                                                      self.config.ioid),
                                                                                  startcol=1, startrow=8, index=False,
                                                                                  header=True)
        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.sqlscript.read_sql_preroll_video.empty:
                pass
            else:
                writing_video_preroll_summary_mv_final = self.videoprerollsummarymvfinal.to_excel(self.config.writer,
                                                                                                  sheet_name="Standard Pre Roll Details".format(
                                                                                                      self.config.ioid),
                                                                                                  startcol=1,
                                                                                                  startrow=len(
                                                                                                      self.prerollsummaryfinal) + 13,
                                                                                                  index=False,
                                                                                                  header=True)
        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.video_player_summary_final.empty:
                pass
            else:
                writing_video_player_final = self.video_player_summary_final.to_excel(self.config.writer,
                                                                                      sheet_name="Standard Pre Roll Details".format(
                                                                                          self.config.ioid),
                                                                                      startcol=1, startrow=len(
                        self.prerollsummaryfinal) + len(self.videoprerollsummarymvfinal) + 18
                                                                                      , index=False, header=True)
        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.sqlscript.read_sql_preroll_interaction.empty:
                pass
            else:
                writing_intraction_player_final = self.intraction_final.to_excel(self.config.writer,
                                                                                 sheet_name="Standard Pre Roll Details".format(
                                                                                     self.config.ioid),
                                                                                 startcol=
                                                                                 self.video_player_summary_final.shape[
                                                                                     1] + 1,
                                                                                 startrow=len(
                                                                                     self.prerollsummaryfinal) + len(
                                                                                     self.videoprerollsummarymvfinal) + 18,
                                                                                 index=False, header=True)
        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

        try:
            if self.dayprerollsummaryfinal.empty:
                pass
            else:
                start_line = len(self.prerollsummaryfinal) + len(self.videoprerollsummarymvfinal) + len(
                    self.intraction_final) + 23
                for placement, placement_df in self.dayprerollsummaryfinal.groupby('Placement# Name', sort= False):

                    writing_day_preroll_summary_final = placement_df.to_excel(self.config.writer,
                                                                              sheet_name="Standard Pre Roll Details".format(
                                                                                  self.config.ioid),
                                                                              startcol=1, startrow=start_line,
                                                                              index=False, header=False,
                                                                              merge_cells=False)

                    workbook = self.config.writer.book
                    worksheet = self.config.writer.sheets["Standard Pre Roll Details".format(self.config.ioid)]
                    start_line += len(placement_df) + 2
                    worksheet.write_string(start_line - 2, 1, 'Subtotal')
                    start_row = start_line - len(placement_df)
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

                    worksheet.write_formula(start_line - 2, 3, '=sum(D{}:D{})'.format(start_row - 1, start_line - 2),
                                            format_num)
                    worksheet.write_formula(start_line - 2, 4, '=sum(E{}:E{})'.format(start_row - 1, start_line - 2),
                                            format_num)
                    worksheet.write_formula(start_line - 2, 5,
                                            '=IFERROR((E{}/D{}),0)'.format(start_line - 1, start_line - 1), percent_fmt)
                    worksheet.write_formula(start_line - 2, 6, '=sum(G{}:G{})'.format(start_row - 1, start_line - 2),
                                            format_num)
                    worksheet.write_formula(start_line - 2, 7,
                                            '=IFERROR((G{}/D{}),0)'.format(start_line - 1, start_line - 1), percent_fmt)

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.write_formula(start_line - 2, 8,
                                                '=sum(I{}:I{})'.format(start_row - 1, start_line - 2),
                                                money_fmt_zar)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.write_formula(start_line - 2, 8,
                                                '=sum(I{}:I{})'.format(start_row - 1, start_line - 2),
                                                money_fmt_mxn)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.write_formula(start_line - 2, 8,
                                                '=sum(I{}:I{})'.format(start_row - 1, start_line - 2),
                                                money_fmt_thb)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.write_formula(start_line - 2, 8,
                                                '=sum(I{}:I{})'.format(start_row - 1, start_line - 2),
                                                money_fmt_eur)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.write_formula(start_line - 2, 8,
                                                '=sum(I{}:I{})'.format(start_row - 1, start_line - 2),
                                                money_fmt_gbp)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.write_formula(start_line - 2, 8,
                                                '=sum(I{}:I{})'.format(start_row - 1, start_line - 2),
                                                money_fmt_inr)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.write_formula(start_line - 2, 8,
                                                '=sum(I{}:I{})'.format(start_row - 1, start_line - 2),
                                                money_fmt_myr)

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.write_formula(start_line - 2, 8,
                                                '=sum(I{}:I{})'.format(start_row - 1, start_line - 2),
                                                money_fmt_chf)

                    else:
                        worksheet.write_formula(start_line - 2, 8,
                                                '=sum(I{}:I{})'.format(start_row - 1, start_line - 2),
                                                money_fmt)

                    worksheet.conditional_format(start_row - 2, 3, start_line, 4,
                                                 {"type": "no_blanks", "format": format_num})
                    worksheet.conditional_format(start_row - 2, 5, start_line, 5,
                                                 {"type": "no_blanks", "format": percent_fmt})
                    worksheet.conditional_format(start_row - 2, 6, start_line, 6,
                                                 {"type": "no_blanks", "format": format_num})
                    worksheet.conditional_format(start_row - 2, 7, start_line, 7,
                                                 {"type": "no_blanks", "format": percent_fmt})

                    if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                        worksheet.conditional_format(start_row - 2, 8, start_line, 8,
                                                     {"type": "no_blanks", "format": money_fmt_zar})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                        worksheet.conditional_format(start_row - 2, 8, start_line, 8,
                                                     {"type": "no_blanks", "format": money_fmt_mxn})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                        worksheet.conditional_format(start_row - 2, 8, start_line, 8,
                                                     {"type": "no_blanks", "format": money_fmt_thb})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                        worksheet.conditional_format(start_row - 2, 8, start_line, 8,
                                                     {"type": "no_blanks", "format": money_fmt_eur})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                        worksheet.conditional_format(start_row - 2, 8, start_line, 8,
                                                     {"type": "no_blanks", "format": money_fmt_gbp})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                        worksheet.conditional_format(start_row - 2, 8, start_line, 8,
                                                     {"type": "no_blanks", "format": money_fmt_inr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                        worksheet.conditional_format(start_row - 2, 8, start_line, 8,
                                                     {"type": "no_blanks", "format": money_fmt_myr})

                    elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                        worksheet.conditional_format(start_row - 2, 8, start_line, 8,
                                                     {"type": "no_blanks", "format": money_fmt_chf})
                    else:
                        worksheet.conditional_format(start_row - 2, 8, start_line, 8,
                                                     {"type": "no_blanks", "format": money_fmt})

        except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass

    def formatting(self):

        """
        Applying Formatting
        """

        try:
            workbook = self.config.writer.book
            worksheet = self.config.writer.sheets["Standard Pre Roll Details".format(self.config.ioid)]
            worksheet.set_zoom(75)

            unique_day_preroll_summary_final = self.dayprerollsummaryfinal["Placement# Name"].nunique()
            number_rows_preroll_summary = self.prerollsummaryfinal.shape[0]
            number_cols_preroll_summary = self.prerollsummaryfinal.shape[1]
            number_rows_video_preroll_summary = self.videoprerollsummarymvfinal.shape[0]
            number_cols_interaction_final = self.intraction_final.shape[1]
            number_rows_day_preroll_summary = self.dayprerollsummaryfinal.shape[0]
            number_cols_day_preroll_summary = self.dayprerollsummaryfinal.shape[1]
            number_rows_video_player_summary_final = self.video_player_summary_final.shape[0]
            number_cols_video_player_summary_final = self.video_player_summary_final.shape[1]

            worksheet.hide_gridlines(2)
            worksheet.set_row(0, 6)
            worksheet.set_column("A:A", 2)

            alignment_center = workbook.add_format({"align": "center"})
            alignment_left = workbook.add_format({"align": "left"})
            alignment_right = workbook.add_format({"align": "right"})

            worksheet.insert_image("O7", "Exponential.png", {"url": "https://www.tribalfusion.com"})
            worksheet.insert_image("O2", "Client_Logo.png")

            worksheet.write_string(2, 8, self.config.status)
            worksheet.write_string(2, 7, "Campaign Status")
            # worksheet.write_string (3, 1, "Agency Name")
            # worksheet.write_string (3, 7, "Currency")

            format_campaign_info = workbook.add_format({"bold": True, "bg_color": '#00B0F0', "align": "left"})
            format_header_left = workbook.add_format({"bold": True, "bg_color": '#00B0F0', "align": "left"})
            format_header_center = workbook.add_format({"bold": True, "bg_color": '#00B0F0', "align": "center"})
            format_header = workbook.add_format({"bold": True, "bg_color": "#00B0F0"})
            format_header_right = workbook.add_format({"bold": True, "bg_color": "#00B0F0", "align": "right"})
            format_grand = workbook.add_format({"bold": True, "bg_color": "#A5A5A5"})
            format_colour = workbook.add_format({"bg_color": '#00B0F0'})

            worksheet.conditional_format("A1:R5", {"type": "blanks", "format": format_campaign_info})
            worksheet.conditional_format("A1:R5", {"type": "no_blanks", "format": format_campaign_info})

            money_fmt = workbook.add_format({"num_format": "$#,###0.00", "align": "right"})
            money_fmt_mxn = workbook.add_format({"num_format": '"MXN" #,###0.00', "align": "right"})
            money_fmt_zar = workbook.add_format({"num_format": '"ZAR" #,###0.00', "align": "right"})
            money_fmt_chf = workbook.add_format({"num_format": '"CHF" #,###0.00', "align": "right"})
            money_fmt_inr = workbook.add_format({"num_format": u'₹#,###0.00', "align": "right"})
            money_fmt_myr = workbook.add_format({"num_format": '"MYR" #,###0.00', "align": "right"})
            money_fmt_thb = workbook.add_format({"num_format": '"THB" #,###0.00', "align": "right"})
            money_fmt_eur = workbook.add_format({"num_format": u'€#,###0.00', "align": "right"})
            money_fmt_gbp = workbook.add_format({"num_format": u'£#,###0.00', "align": "right"})

            money_fmt_spend = workbook.add_format({"num_format": "$#,###0.00", "align": "right", "bg_color": "#A5A5A5"})
            money_fmt_mxn_spend = workbook.add_format(
                {"num_format": '"MXN" #,###0.00', "align": "right", "bg_color": "#A5A5A5"})
            money_fmt_zar_spend = workbook.add_format(
                {"num_format": '"ZAR" #,###0.00', "align": "right", "bg_color": "#A5A5A5"})
            money_fmt_chf_spend = workbook.add_format(
                {"num_format": '"CHF" #,###0.00', "align": "right", "bg_color": "#A5A5A5"})
            money_fmt_inr_spend = workbook.add_format(
                {"num_format": u'₹#,###0.00', "align": "right", "bg_color": "#A5A5A5"})
            money_fmt_myr_spend = workbook.add_format(
                {"num_format": '"MYR" #,###0.00', "align": "right", "bg_color": "#A5A5A5"})
            money_fmt_thb_spend = workbook.add_format(
                {"num_format": '"THB" #,###0.00', "align": "right", "bg_color": "#A5A5A5"})
            money_fmt_eur_spend = workbook.add_format(
                {"num_format": u'€#,###0.00', "align": "right", "bg_color": "#A5A5A5"})
            money_fmt_gbp_spend = workbook.add_format(
                {"num_format": u'£#,###0.00', "align": "right", "bg_color": "#A5A5A5"})

            format_num = workbook.add_format({"num_format": "#,##0"})
            format_num_cent = workbook.add_format(
                {"num_format": "#,##0", "align": "center", "bg_color": "#A5A5A5", "bold": True})
            percent_fmt = workbook.add_format({"num_format": "0.00%", "align": "right"})
            percent_fmt_ctr_vcr = workbook.add_format({"num_format": "0.00%", "align": "right", "bg_color": "#A5A5A5"})

            worksheet.write_string(7, 1, "Standard Pre Roll Performance - Summary", format_header_left)
            worksheet.write_string(9 + number_rows_preroll_summary, 1, "Grand Total", format_grand)
            worksheet.conditional_format(7, 2, 7, number_cols_preroll_summary,
                                         {"type": "blanks", "format": format_colour})
            worksheet.conditional_format(7, 2, 7, number_cols_preroll_summary,
                                         {"type": "no_blanks", "format": format_colour})
            worksheet.conditional_format(8, 1, 8, 1, {"type": "no_blanks", "format": format_header_left})

            for col in range(2, number_cols_preroll_summary + 1):
                worksheet.write_string(7, col, "", format_colour)
                worksheet.conditional_format(8, col, 8, col, {"type": "no_blanks", "format": format_header})
                worksheet.conditional_format(8, col, 8, col, {"type": "blanks", "format": format_header})

            for col in range(4, 6):
                cell_location = xl_rowcol_to_cell(9 + number_rows_preroll_summary, col)
                start_range = xl_rowcol_to_cell(9, col)
                end_range = xl_rowcol_to_cell(9 + number_rows_preroll_summary - 1, col)
                formula = '=sum({:s}:{:s})'.format(start_range, end_range)
                worksheet.write_formula(cell_location, formula, format_num)
                start_plc_row = 9
                end_plc_row = 9 + number_rows_preroll_summary - 1
                worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                             {"type": "no_blanks", "format": format_num})
                start_range_format = 9 + number_rows_preroll_summary
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "no_blanks", "format": format_grand})
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "blanks", "format": format_grand})

            for col in range(6, 7):
                cell_location = xl_rowcol_to_cell(9 + number_rows_preroll_summary, col)
                formula = '=IFERROR(F{}/E{},0)'.format(9 + number_rows_preroll_summary + 1,
                                                       9 + number_rows_preroll_summary + 1)
                worksheet.write_formula(cell_location, formula, percent_fmt)
                start_plc_row = 9
                end_plc_row = 9 + number_rows_preroll_summary - 1
                worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                             {"type": "no_blanks", "format": percent_fmt})
                start_range_format = 9 + number_rows_preroll_summary
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "no_blanks", "format": format_grand})
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "blanks", "format": format_grand})

            for col in range(7, 8):
                cell_location = xl_rowcol_to_cell(9 + number_rows_preroll_summary, col)
                start_range = xl_rowcol_to_cell(9, col)
                end_range = xl_rowcol_to_cell(9 + number_rows_preroll_summary - 1, col)
                formula = '=sum({:s}:{:s})'.format(start_range, end_range)
                worksheet.write_formula(cell_location, formula, format_num)
                start_plc_row = 9
                end_plc_row = 9 + number_rows_preroll_summary - 1
                worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                             {"type": "no_blanks", "format": format_num})
                start_range_format = 9 + number_rows_preroll_summary
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "no_blanks", "format": format_grand})
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "blanks", "format": format_grand})

            for col in range(8, 9):
                cell_location = xl_rowcol_to_cell(9 + number_rows_preroll_summary, col)
                formula = '=IFERROR(H{}/E{},0)'.format(9 + number_rows_preroll_summary + 1,
                                                       9 + number_rows_preroll_summary + 1)
                worksheet.write_formula(cell_location, formula, percent_fmt)
                start_plc_row = 9
                end_plc_row = 9 + number_rows_preroll_summary - 1
                worksheet.conditional_format(start_plc_row, col, end_plc_row, col,
                                             {"type": "no_blanks", "format": percent_fmt})
                start_range_format = 9 + number_rows_preroll_summary
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "no_blanks", "format": format_grand})
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "blanks", "format": format_grand})

            for col in range(9, 10):
                cell_location = xl_rowcol_to_cell(9 + number_rows_preroll_summary, col)
                start_range = xl_rowcol_to_cell(9, col)
                end_range = xl_rowcol_to_cell(9 + number_rows_preroll_summary - 1, col)
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
                end_plc_row = 9 + number_rows_preroll_summary - 1

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

                start_range_format = 9 + number_rows_preroll_summary
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "no_blanks", "format": format_grand})
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "blanks", "format": format_grand})

            for col in range(3, 4):
                start_plc_row = 9
                end_plc_row = 9 + number_rows_preroll_summary - 1

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

                start_range = 9 + number_rows_preroll_summary
                worksheet.conditional_format(start_range, col, start_range, col,
                                             {"type": "blanks", "format": format_grand})
                worksheet.conditional_format(start_range, col, start_range, col,
                                             {"type": "no_blanks", "format": format_grand})

            for col in range(2, 3):
                start_range_format = 9 + number_rows_preroll_summary
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "blanks", "format": format_grand})
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "no_blanks", "format": format_grand})

            worksheet.write_string(9 + number_rows_preroll_summary + 3, 1, "Standard Pre Roll - Video Details",
                                   format_header_left)
            worksheet.conditional_format(13 + number_rows_preroll_summary, 1, 13 + number_rows_preroll_summary, 1,
                                         {"type": "no_blanks", "format": format_header_left})
            worksheet.write_string(13 + number_rows_preroll_summary + number_rows_video_player_summary_final + 1, 1,
                                   "Grand Total", format_grand)

            for col in range(2, number_cols_video_player_summary_final + 1):
                worksheet.write_string(12 + number_rows_preroll_summary, col, "", format_colour)
                worksheet.conditional_format(13 + number_rows_preroll_summary, col, 13 + number_rows_preroll_summary,
                                             col,
                                             {"type": "no_blanks", "format": format_header})

                worksheet.conditional_format(13 + number_rows_preroll_summary, col, 13 + number_rows_preroll_summary,
                                             col,
                                             {"type": "blanks", "format": format_header})

            for col in range(2, 3):
                start_range_format = 13 + number_rows_preroll_summary + number_rows_video_player_summary_final + 1
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "blanks", "format": format_grand})
                worksheet.conditional_format(start_range_format, col, start_range_format, col,
                                             {"type": "no_blanks", "format": format_grand})

            for col in range(3, 8):
                cell_location = xl_rowcol_to_cell(
                    13 + number_rows_preroll_summary + number_rows_video_player_summary_final + 1, col)
                start_range = xl_rowcol_to_cell(14 + number_rows_preroll_summary, col)
                end_range = xl_rowcol_to_cell(13 + number_rows_preroll_summary + number_rows_video_player_summary_final,
                                              col)
                formula = '=sum({:s}:{:s})'.format(start_range, end_range)
                worksheet.write_formula(cell_location, formula, format_num)

                start_range_plc = 14 + number_rows_preroll_summary
                end_range_plc = 13 + number_rows_preroll_summary + number_rows_video_player_summary_final
                worksheet.conditional_format(start_range_plc, col, end_range_plc, col,
                                             {"type": "no_blanks", "format": format_num})
                start_range_format = 13 + number_rows_preroll_summary + number_rows_video_player_summary_final + 1
                worksheet.conditional_format(start_range_format, col, start_range_format, col, {"type": "no_blanks",
                                                                                                "format": format_grand})

                worksheet.conditional_format(start_range_format, col, start_range_format, col, {"type": "blanks",
                                                                                                "format": format_grand})

            for col in range(8, 9):
                cell_location = xl_rowcol_to_cell(
                    13 + number_rows_preroll_summary + number_rows_video_player_summary_final + 1, col)
                formula = '=IFERROR(H{}/D{},0)'.format(
                    13 + number_rows_preroll_summary + number_rows_video_player_summary_final + 2,
                    13 + number_rows_preroll_summary + number_rows_video_player_summary_final + 2)
                worksheet.write_formula(cell_location, formula, percent_fmt)
                start_range_plc = 14 + number_rows_preroll_summary
                end_range_plc = 13 + number_rows_preroll_summary + number_rows_video_player_summary_final
                worksheet.conditional_format(start_range_plc, col, end_range_plc, col,
                                             {"type": "no_blanks", "format": percent_fmt})

                start_range_format = 13 + number_rows_preroll_summary + number_rows_video_player_summary_final + 1
                worksheet.conditional_format(start_range_format, col, start_range_format, col, {
                    "type": "no_blanks",
                    "format": format_grand
                })

                worksheet.conditional_format(start_range_format, col, start_range_format, col, {
                    "type": "blanks",
                    "format": format_grand
                })

            worksheet.write_string(16 + number_rows_preroll_summary + number_rows_video_preroll_summary, 1,
                                   "Standard Pre Roll - Interaction Details", format_header_left)
            worksheet.write_string(17 + number_rows_preroll_summary + number_rows_video_preroll_summary, 2,
                                   "Video Player Interactions", format_header)
            worksheet.write_string(17 + number_rows_preroll_summary + number_rows_video_preroll_summary,
                                   number_cols_video_player_summary_final + 1, "Clickthroughs", format_header)
            worksheet.write_string(
                19 + number_rows_preroll_summary + number_rows_video_preroll_summary + number_rows_video_player_summary_final,
                1,
                "Grand Total", format_grand)

            worksheet.write_string(18 + number_rows_preroll_summary + number_rows_video_preroll_summary,
                                   number_cols_video_player_summary_final + number_cols_interaction_final + 1,
                                   "Total Interactions", format_header_right)

            for col in range(1, number_cols_video_player_summary_final + number_cols_interaction_final + 2):
                start_range = 16 + number_rows_preroll_summary + number_rows_video_preroll_summary
                end_range = 17 + number_rows_preroll_summary + number_rows_video_preroll_summary
                start_range_header = 18 + number_rows_preroll_summary + number_rows_video_preroll_summary
                worksheet.conditional_format(start_range, col, end_range, col,
                                             {"type": "blanks", "format": format_colour})
                worksheet.conditional_format(start_range, col, end_range, col,
                                             {"type": "no_blanks", "format": format_colour})
                worksheet.conditional_format(start_range_header, col, start_range_header, col,
                                             {"type": "no_blanks", "format": format_header})

            for col in range(3, number_cols_video_player_summary_final + number_cols_interaction_final + 2):
                cell_location = xl_rowcol_to_cell(
                    19 + number_rows_preroll_summary + number_rows_video_preroll_summary + number_rows_video_player_summary_final,
                    col)
                start_range = xl_rowcol_to_cell(19 + number_rows_preroll_summary + number_rows_video_preroll_summary,
                                                col)
                end_range = xl_rowcol_to_cell(
                    19 + number_rows_preroll_summary + number_rows_video_preroll_summary + number_rows_video_player_summary_final - 1,
                    col)
                formula = '=sum({:s}:{:s})'.format(start_range, end_range)
                worksheet.write_formula(cell_location, formula, format_num)
                start_range_plc = 19 + number_rows_preroll_summary + number_rows_video_preroll_summary
                end_range_plc = 19 + number_rows_preroll_summary + number_rows_video_preroll_summary + number_rows_video_player_summary_final - 1
                worksheet.conditional_format(start_range_plc, col, end_range_plc, col,
                                             {"type": "no_blanks", "format": format_num})
                range_grand = 19 + number_rows_preroll_summary + number_rows_video_preroll_summary + number_rows_video_player_summary_final
                worksheet.conditional_format(range_grand, col, range_grand, col,
                                             {"type": "no_blanks", "format": format_grand})

            for col in range(2, 3):
                cell_location = xl_rowcol_to_cell(19 + number_rows_preroll_summary + number_rows_video_preroll_summary
                                                  + number_rows_video_player_summary_final,
                                                  col)
                start_range = xl_rowcol_to_cell(19 + number_rows_preroll_summary + number_rows_video_preroll_summary,
                                                col)
                end_range = xl_rowcol_to_cell(
                    19 + number_rows_preroll_summary + number_rows_video_preroll_summary
                    + number_rows_video_player_summary_final - 1,
                    col)
                formula = '=sum({:s}:{:s})'.format(start_range, end_range)
                worksheet.write_formula(cell_location, formula, format_num_cent)
                start_range_plc = 19 + number_rows_preroll_summary + number_rows_video_preroll_summary
                end_range_plc = 19 + number_rows_preroll_summary + number_rows_video_preroll_summary \
                                + number_rows_video_player_summary_final - 1
                worksheet.conditional_format(start_range_plc, col, end_range_plc, col,
                                             {"type": "no_blanks", "format": format_num})

            start_range_video = 19 + number_rows_preroll_summary + number_rows_video_preroll_summary
            start_col_video = number_cols_video_player_summary_final + number_cols_interaction_final + 1
            for row in range(number_rows_video_player_summary_final):
                cell_range = xl_range(start_range_video, 2, start_range_video,
                                      number_cols_video_player_summary_final + number_cols_interaction_final)

                formula = 'sum({:s})'.format(cell_range)
                worksheet.write_formula(start_range_video, start_col_video, formula)
                start_range_video += 1

            worksheet.write_string(21 + number_rows_preroll_summary +
                                   number_rows_video_preroll_summary +
                                   number_rows_video_player_summary_final, 1, "Standard Pre Roll - by Date",
                                   format_header_left)

            worksheet.write_string(22 + number_rows_preroll_summary +
                                   number_rows_video_preroll_summary +
                                   number_rows_video_player_summary_final, 1, "Placement # Name",
                                   format_header_left)

            worksheet.write_string(22 + number_rows_preroll_summary +
                                   number_rows_video_preroll_summary +
                                   number_rows_video_player_summary_final, 2, "Date",
                                   format_header_center)

            worksheet.write_string(22 + number_rows_preroll_summary +
                                   number_rows_video_preroll_summary +
                                   number_rows_video_player_summary_final, 3, "Impressions", format_header_right)

            worksheet.write_string(22 + number_rows_preroll_summary +
                                   number_rows_video_preroll_summary +
                                   number_rows_video_player_summary_final, 4, "Clickthroughs", format_header_right)

            worksheet.write_string(22 + number_rows_preroll_summary +
                                   number_rows_video_preroll_summary +
                                   number_rows_video_player_summary_final, 5, "CTR %", format_header_right)

            worksheet.write_string(22 + number_rows_preroll_summary +
                                   number_rows_video_preroll_summary +
                                   number_rows_video_player_summary_final, 6, "Video Completions", format_header_right)

            worksheet.write_string(22 + number_rows_preroll_summary +
                                   number_rows_video_preroll_summary +
                                   number_rows_video_player_summary_final, 7, "VCR %", format_header_right)

            worksheet.write_string(22 + number_rows_preroll_summary +
                                   number_rows_video_preroll_summary +
                                   number_rows_video_player_summary_final, 8, "Spend", format_header_right)

            start_range_day = 21 + number_rows_preroll_summary + number_rows_video_preroll_summary + \
                              number_rows_video_player_summary_final

            for col in range(2, number_cols_day_preroll_summary + 1):
                worksheet.conditional_format(start_range_day, col, start_range_day, col,
                                             {"type": "blanks", "format": format_colour})
                worksheet.conditional_format(start_range_day, col, start_range_day, col,
                                             {"type": "no_blanks", "format": format_colour})

            start_grand_total = 21 + number_rows_preroll_summary + \
                                number_rows_video_preroll_summary + \
                                number_rows_day_preroll_summary + \
                                number_rows_video_player_summary_final + \
                                unique_day_preroll_summary_final * 2 + 1

            worksheet.write_string(start_grand_total, 1, "Grand Total", format_grand)
            worksheet.write_string(start_grand_total, 2, "", format_grand)

            cell_location = start_grand_total
            start_range_day = 24 + number_rows_preroll_summary + number_rows_video_preroll_summary + number_rows_video_player_summary_final
            end_range_day = start_grand_total
            formula_imp = '=SUMIF(B{}:B{},"Subtotal",D{}:D{})'.format(start_range_day, end_range_day, start_range_day,
                                                                      end_range_day)
            formula_click = '=SUMIF(B{}:B{},"Subtotal",E{}:E{})'.format(start_range_day, end_range_day, start_range_day,
                                                                        end_range_day)
            formula_comp = '=SUMIF(B{}:B{},"Subtotal",G{}:G{})'.format(start_range_day, end_range_day, start_range_day,
                                                                       end_range_day)

            formula_spend = '=SUMIF(B{}:B{},"Subtotal",I{}:I{})'.format(start_range_day, end_range_day, start_range_day,
                                                                        end_range_day)

            formula_ctr = '=IFERROR((E{}/D{}),0)'.format(start_grand_total + 1, start_grand_total + 1)
            formula_vcr = '=IFERROR((G{}/D{}),0)'.format(start_grand_total + 1, start_grand_total + 1)

            worksheet.write_formula(cell_location, 3, formula_imp, format_grand)
            worksheet.write_formula(cell_location, 4, formula_click, format_grand)
            worksheet.write_formula(cell_location, 5, formula_ctr, percent_fmt_ctr_vcr)
            worksheet.write_formula(cell_location, 6, formula_comp, format_grand)
            worksheet.write_formula(cell_location, 7, formula_vcr, percent_fmt_ctr_vcr)

            if self.config.cdb_value_currency.iloc[0, 0] == 'ZAR':
                worksheet.write_formula(cell_location, 8, formula_spend, money_fmt_zar_spend)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'MXN':
                worksheet.write_formula(cell_location, 8, formula_spend, money_fmt_mxn_spend)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'THB':
                worksheet.write_formula(cell_location, 8, formula_spend, money_fmt_thb_spend)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'EUR':
                worksheet.write_formula(cell_location, 8, formula_spend, money_fmt_eur_spend)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'GBP':
                worksheet.write_formula(cell_location, 8, formula_spend, money_fmt_gbp_spend)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'INR':
                worksheet.write_formula(cell_location, 8, formula_spend, money_fmt_inr_spend)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'MYR':
                worksheet.write_formula(cell_location, 8, formula_spend, money_fmt_myr_spend)

            elif self.config.cdb_value_currency.iloc[0, 0] == 'CHF':
                worksheet.write_formula(cell_location, 8, formula_spend, money_fmt_chf_spend)

            else:
                worksheet.write_formula(cell_location, 8, formula_spend, money_fmt_spend)

            worksheet.set_column(1, 1, 45, alignment_left)
            worksheet.set_column(2, 2, 15, alignment_center)
            worksheet.set_column(3, number_cols_interaction_final + 18, 20, alignment_right)
            worksheet.set_row(1,None,alignment_left)
            worksheet.set_row(2, None, alignment_left)
            worksheet.set_row(3, None, alignment_left)

        except(AttributeError, KeyError, TypeError, IOError, ValueError) as e:
            self.logger.error(str(e))
            pass


    def main(self):
        """
        main function
        """
        self.config.common_columns_summary()
        self.config.logger.info(
            "Start Creating Standard Pre Roll Details Sheet for IO - {} ".format(self.config.ioid) + " at " + str(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        if self.sqlscript.read_sql_preroll.empty :
            self.logger.info("No In-stream placements for IO - {}".format(self.config.ioid) + " at "+ str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
            pass
        else:
            self.logger.info("Live In-stream placements found for IO - {}".format(self.config.ioid) + " at "+ str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
            self.accessing_preroll_columns()
            self.renameIntraction()
            self.writePreroll()
            self.formatting()
            self.logger.info('Standard Pre Roll Details Sheet created for IO {}'.format(self.config.ioid) + " at "+ str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))


if __name__ == "__main__":
    pass

# enable it when running for individual file
# c = config.Config('Test', 608607,'2018-01-01','2018-02-01')
# o = Intraction( c )
# o.main()
# c.saveAndCloseWriter()
