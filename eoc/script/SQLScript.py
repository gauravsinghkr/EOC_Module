# !/usr/bin/python
# coding=utf-8
"""
SQL Script
"""

import datetime
import pandas as pd
import cx_Oracle as OraCx
from EOC_Module.eoc.script.BiProper import Properties


class SqlScript(Properties):
    """
    This is SQL Class
    """
    def __init__(self, config):

        super(SqlScript, self).__init__()
        self.config = config
        self.connect = None
        self.read_sql__display = None
        self.read_sql__display_mv = None
        self.read_sql__display_placement = None
        self.read_sql__v_d_x = None
        self.read_sql__v_d_x_mv = None
        self.read_sql_preroll = None
        self.read_sql_preroll_mv = None
        self.read_sql_adsize_mv = None
        self.read_sql_daily_mv = None
        self.read_sql_preroll_video = None
        self.read_sql_video_details = None
        self.read_sql_preroll_video_player = None
        self.read_sql_preroll_interaction = None
        self.read_sql_preroll_day = None
        self.read_sql_adsize_km = None
        self.read_sql_video_km = None
        self.read_sql_km_for_video = None
        self.read_sql_video_player_interaction = None
        self.read_sql_ad_intraction = None
        self.read_sql_video_day = None
        self.read_sql__v_d_x_placement = None
        self.read_sql_preroll_placement = None

    def connection(self):
        """connection on TFR"""
        self.connect = OraCx.connect(self.login)
        return self.connect

    def sql_display_summary(self):
        """Display_Summary"""
        self.config.logger.info("Start executing: " + 'Display_Summary.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_display_summary = open(self.sql + 'Display_Summary.sql')
        sqldisplaysummary = read_display_summary.read().format(self.config.ioid, self.config.start_date, self.config.end_date)
        read_sql__display = pd.read_sql(sqldisplaysummary, self.connection())
        self.read_sql__display = read_sql__display

    def sql_placement_info_display(self):
        """Display information"""
        self.config.logger.info("Start executing: " + 'Placement_info_display.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_display_placement = open(self.sql + 'Placement_info_display.sql')
        sqldisplayplacement = read_display_placement.read().format(self.config.ioid, self.config.start_date, self.config.end_date)
        read_sql_display_placement = pd.read_sql(sqldisplayplacement,self.connection())
        self.read_sql__display_placement = read_sql_display_placement

    def sql_display_mv(self):
        """Display_MV"""
        self.config.logger.info("Start executing: " + 'Display_MV.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_display_mv = open(self.sql + 'Display_MV.sql')
        sqldisplaymv = read_display_mv.read().format(self.config.ioid, self.config.start_date, self.config.end_date)
        read_sql__display_mv = pd.read_sql(sqldisplaymv, self.connection())
        self.read_sql__display_mv = read_sql__display_mv

    def sql_display_ad_size(self):
        """Display Ad_Size"""
        self.config.logger.info("Start executing: " + 'Placement_info_display_adsize.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_adsize_display = open(self.sql + 'Placement_info_display_adsize.sql')
        sql_sales_adsize_mv = read_adsize_display.read().format(self.config.ioid, self.config.start_date,self.config.end_date)
        read_sql_adsize_mv = pd.read_sql(sql_sales_adsize_mv, self.connection())
        self.read_sql_adsize_mv = read_sql_adsize_mv

    def sql_display_daily(self):
        """Display Day"""
        self.config.logger.info("Start executing: " + 'Placement_info_display_day.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_by_day_display = open(self.sql + 'Placement_info_display_day.sql')
        sql_sales_daily_mv = read_by_day_display.read().format(self.config.ioid, self.config.start_date,self.config.end_date)
        read_sql_daily_mv = pd.read_sql(sql_sales_daily_mv, self.connection())
        self.read_sql_daily_mv = read_sql_daily_mv

    def sql_vdx_summary(self):
        """VDX_Summary"""
        self.config.logger.info("Start executing: " + 'VDX_Summary.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_vdx_summary = open(self.sql + 'VDX_Summary.sql')
        sqlvdxsummary = read_vdx_summary.read().format(self.config.ioid, self.config.start_date, self.config.end_date)
        read_sql__v_d_x = pd.read_sql(sqlvdxsummary, self.connection())
        self.read_sql__v_d_x = read_sql__v_d_x

    def sql_placement_info_vdx(self):
        """VDX Placement Information"""
        self.config.logger.info("Start executing: " + 'Placement_info_vdx.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_vdx_placement = open(self.sql + 'Placement_info_vdx.sql')
        sqlvdxplacement = read_vdx_placement.read().format(self.config.ioid, self.config.start_date, self.config.end_date)
        read_sql__v_d_x_placement = pd.read_sql(sqlvdxplacement, self.connection())
        self.read_sql__v_d_x_placement = read_sql__v_d_x_placement

    def sql_vdx_mv(self):
        """VDX_MV"""
        self.config.logger.info("Start executing: " + 'VDX_MV.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_vdx_mv = open(self.sql + 'VDX_MV.sql')
        sqlvdxmv = read_vdx_mv.read().format(self.config.ioid, self.config.start_date, self.config.end_date)
        read_sql__v_d_x_mv = pd.read_sql(sqlvdxmv, self.connection())
        self.read_sql__v_d_x_mv = read_sql__v_d_x_mv

    def sql_vdx_adsize(self):
        """VDX_Ad Size"""
        self.config.logger.info("Start executing: " + 'Placement_info_vdx_adsize.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_adsize_vdx = open(self.sql + 'Placement_info_vdx_adsize.sql')
        sql_adsize_km = read_adsize_vdx.read().format(self.config.ioid, self.config.start_date, self.config.end_date)
        read_sql_adsize_km = pd.read_sql(sql_adsize_km, self.connection())
        self.read_sql_adsize_km = read_sql_adsize_km

    def sql_vdx_video_details(self):
        """VDX_Video Details"""
        self.config.logger.info("Start executing: " + 'Placement_info_vdx_video.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_video_vdx = open(self.sql + 'Placement_info_vdx_video.sql')
        sql_video_km = read_video_vdx.read().format(self.config.ioid, self.config.start_date, self.config.end_date)
        read_sql_video_km = pd.read_sql(sql_video_km, self.connection())
        self.read_sql_video_km = read_sql_video_km

    def sql_vdx_key_metric_view(self):
        """Key Metric Impression, ENG, Deep"""
        self.config.logger.info("Start executing: " + 'Placement_info_vdx_km.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_vdx_km_video = open(self.sql + 'Placement_info_vdx_km.sql')
        sql_km_for_video = read_vdx_km_video.read().format(self.config.ioid, self.config.start_date,self.config.end_date)
        read_sql_km_for_video = pd.read_sql(sql_km_for_video, self.connection())
        self.read_sql_km_for_video = read_sql_km_for_video

    def sql_vdx_interaction_video_details(self):
        """Interactions from Video Details View"""
        self.config.logger.info("Start executing: " + 'Placement_info_vdx_intraction.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_video_intraction = open(self.sql + 'Placement_info_vdx_intraction.sql')
        sql_video_player_intraction = read_video_intraction.read().format(self.config.ioid, self.config.start_date, self.config.end_date)
        read_sql_video_player_interaction = pd.read_sql(sql_video_player_intraction, self.connection())
        self.read_sql_video_player_interaction = read_sql_video_player_interaction

    def sql_vdx_interaction_details_view(self):
        """Interactions from Interactions Details View"""
        self.config.logger.info("Start executing: " + 'Placement_info_vdx_ad_intraction.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_video_ad_intrac = open(self.sql + 'Placement_info_vdx_ad_intraction.sql')
        sql_ad_intraction = read_video_ad_intrac.read().format(self.config.ioid, self.config.start_date,self.config.end_date)
        read_sql_ad_intraction = pd.read_sql(sql_ad_intraction, self.connection())
        self.read_sql_ad_intraction = read_sql_ad_intraction

    def sql_vdx_key_metric_day(self):
        """Placement By Day Key Metric"""
        self.config.logger.info("Start executing: " + 'Placement_info_vdx_day.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_vdx_day = open(self.sql + 'Placement_info_vdx_day.sql')
        sql_vdx_day_km = read_vdx_day.read().format(self.config.ioid, self.config.start_date, self.config.end_date)
        read_sql_video_day = pd.read_sql(sql_vdx_day_km, self.connection())
        self.read_sql_video_day = read_sql_video_day

    def sql_preroll_summary(self):
        """preroll_Summary"""
        self.config.logger.info("Start executing: " + 'Preroll_Summary.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_preroll_summary = open(self.sql + 'Preroll_Summary.sql')
        sqlprerollsummary = read_preroll_summary.read().format(self.config.ioid, self.config.start_date,self.config.end_date)
        read_sql_preroll = pd.read_sql(sqlprerollsummary, self.connection())
        self.read_sql_preroll = read_sql_preroll

    def sql_preroll_placement(self):
        """preroll placement"""
        self.config.logger.info("Start executing: " + 'Preroll_Summary.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_preroll_placement = open(self.sql + 'Placement_info_preroll.sql')
        sqlprerollplacement = read_preroll_placement.read().format(self.config.ioid, self.config.start_date, self.config.end_date)
        read_sql_preroll_placement = pd.read_sql(sqlprerollplacement, self.connection())
        self.read_sql_preroll_placement = read_sql_preroll_placement

    def sql_preroll_mv(self):
        """preroll_MV"""
        self.config.logger.info("Start executing: " + 'Preroll_MV.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_preroll_mv = open(self.sql + 'Preroll_MV.sql')
        sqlprerollmv = read_preroll_mv.read().format(self.config.ioid, self.config.start_date, self.config.end_date)
        read_sql_preroll_mv = pd.read_sql(sqlprerollmv, self.connection())
        self.read_sql_preroll_mv = read_sql_preroll_mv

    def sql_preroll_video_key_metric(self):
        """preroll_Videos_using_key_metric"""
        self.config.logger.info("Start executing: " + 'Video_info_preroll.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_preroll_video = open(self.sql + 'Video_info_preroll.sql')
        sql_preroll_video_views = read_preroll_video.read().format(self.config.ioid, self.config.start_date,self.config.end_date)
        read_sql_preroll_video = pd.read_sql(sql_preroll_video_views, self.connection())
        self.read_sql_preroll_video = read_sql_preroll_video

    def sql_preroll_video_video_detail(self):
        """preroll_videos_using_video_details"""
        self.config.logger.info("Start executing: " + 'Video_details_info_preroll.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_preroll_video_details = open(self.sql + 'Video_details_info_preroll.sql')
        sql_video_details = read_preroll_video_details.read().format(self.config.ioid, self.config.start_date,self.config.end_date)
        read_sql_video_details = pd.read_sql(sql_video_details, self.connection())
        self.read_sql_video_details = read_sql_video_details

    def sql_preroll_video_details_intraction(self):
        """Interactions Details from Video Details"""
        self.config.logger.info("Start executing: " + 'Placement_player_video_preroll.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_preroll_video = open(self.sql + 'Placement_player_video_preroll.sql')
        sql_preroll_video_player = read_preroll_video.read().format(self.config.ioid, self.config.start_date,self.config.end_date)
        read_sql_preroll_video_player = pd.read_sql(sql_preroll_video_player, self.connection())
        self.read_sql_preroll_video_player = read_sql_preroll_video_player

    def sql_preroll_intraction_details(self):
        """Intraction Click through"""
        self.config.logger.info("Start executing: " + 'Placement_player_int_preroll.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_preroll_intraction = open(self.sql + 'Placement_player_int_preroll.sql')
        sql_preroll_interaction = read_preroll_intraction.read().format(self.config.ioid, self.config.start_date, self.config.end_date)
        read_sql_preroll_interaction = pd.read_sql(sql_preroll_interaction, self.connection())
        self.read_sql_preroll_interaction = read_sql_preroll_interaction

    def sql_preroll_daily(self):
        """Preroll by day"""
        self.config.logger.info("Start executing: " + 'Placement_info_preroll_day.sql' + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
        read_preroll_day = open(self.sql + 'Placement_info_preroll_day.sql')
        sql_preroll_day_mv = read_preroll_day.read().format(self.config.ioid, self.config.start_date,self.config.end_date)
        read_sql_preroll_day = pd.read_sql(sql_preroll_day_mv, self.connection())
        self.read_sql_preroll_day = read_sql_preroll_day

    def main(self):
        """Main Function"""
        self.connection()
        self.config.logger.info("Connected to TFR for IO:- {} ".format(self.config.ioid))
        self.sql_display_summary()
        if self.read_sql__display.empty:
            self.config.logger.info("No Display Placements found for - {}".format(self.config.ioid))
            pass
        else:
            self.sql_placement_info_display()
            self.sql_display_mv()
            self.sql_display_ad_size()
            self.sql_display_daily()

        self.sql_vdx_summary()
        if self.read_sql__v_d_x.empty:
            self.config.logger.info("No VDX Placements found for - {}".format(self.config.ioid))
            pass
        else:
            self.sql_placement_info_vdx()
            self.sql_vdx_mv()
            self.sql_vdx_adsize()
            self.sql_vdx_video_details()
            self.sql_vdx_key_metric_view()
            self.sql_vdx_interaction_video_details()
            self.sql_vdx_interaction_details_view()
            self.sql_vdx_key_metric_day()

        self.sql_preroll_summary()
        if self.read_sql_preroll.empty:
            self.config.logger.info("No Preroll Placements found for - {}".format(self.config.ioid))
            pass
        else:
            self.sql_preroll_placement()
            self.sql_preroll_mv()
            self.sql_preroll_video_key_metric()
            self.sql_preroll_video_video_detail()
            self.sql_preroll_video_details_intraction()
            self.sql_preroll_intraction_details()
            self.sql_preroll_daily()


if __name__ == "__main__":
    pass
    #c= Config('2018-08-08',616117,'2018-08-09')
    #obj_sql = SqlScript(c)
    #obj_sql.main()



