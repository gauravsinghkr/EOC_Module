# coding=utf-8
# !/usr/bin/env python
import pandas as pd
import config
import logging


class Definition(object):
    def __init__(self, config):
        self.config = config
        self.logger = self.config.logger

    def reading_def(self):
        read_definition = pd.read_excel("..//data//EocCommonSheet.xlsx", header=None)
        return read_definition

    def writing_definition(self):
        read_definition = self.reading_def()
        write_defitntion = read_definition.to_excel(self.config.writer,
                                                    sheet_name="Definition".format(self.config.ioid),
                                                    index=False, header=False)
        return write_defitntion

    def format_definition(self):
        workbook = self.config.writer.book
        worksheet = self.config.writer.sheets["Definition".format(self.config.ioid)]

        format_metric_def = workbook.add_format({'bold': True, 'font_size': 16})
        format_vwr_metric = workbook.add_format({'bold': True, 'font_size': 14})
        format_statics = workbook.add_format({'font_size': 12})
        format_range_colour = workbook.add_format({"bg_color": '#F2F2F2'})
        format_colour = workbook.add_format({"bg_color": "#D6DCE4"})
        format_new_colour = workbook.add_format({"bg_color": "#D9D9D9"})

        worksheet.set_row(0, 5)
        worksheet.set_row(4, 21, format_metric_def)
        worksheet.set_row(6, 18, format_vwr_metric)
        worksheet.set_row(8, 18, format_vwr_metric)
        worksheet.set_row(34, 18, format_vwr_metric)
        worksheet.set_row(36, 18, format_vwr_metric)
        worksheet.set_row(54, 18, format_vwr_metric)
        worksheet.set_row(56, 18, format_vwr_metric)
        worksheet.set_row(74, 18, format_vwr_metric)
        worksheet.set_row(75, 18, format_vwr_metric)
        worksheet.set_row(7, 16, format_statics)
        worksheet.set_row(35, 16, format_statics)
        worksheet.set_row(55, 16, format_statics)
        worksheet.conditional_format(1, 1, 5, 2, {"type": "blanks", "format": format_range_colour})
        worksheet.conditional_format(1, 1, 5, 2, {"type": "no_blanks", "format": format_range_colour})
        worksheet.conditional_format(6, 1, 6, 2, {"type": "blanks", "format": format_colour})
        worksheet.conditional_format(6, 1, 6, 2, {"type": "no_blanks", "format": format_colour})
        worksheet.conditional_format(8, 1, 8, 2, {"type": "no_blanks", "format": format_new_colour})
        worksheet.conditional_format(34, 1, 34, 2, {"type": "blanks", "format": format_colour})
        worksheet.conditional_format(34, 1, 34, 2, {"type": "no_blanks", "format": format_colour})
        worksheet.conditional_format(36, 1, 36, 2, {"type": "no_blanks", "format": format_new_colour})
        worksheet.conditional_format(54, 1, 54, 2, {"type": "blanks", "format": format_colour})
        worksheet.conditional_format(54, 1, 54, 2, {"type": "no_blanks", "format": format_colour})
        worksheet.conditional_format(56, 1, 56, 2, {"type": "no_blanks", "format": format_new_colour})
        worksheet.conditional_format(74, 1, 74, 2, {"type": "blanks", "format": format_colour})
        worksheet.conditional_format(74, 1, 74, 2, {"type": "no_blanks", "format": format_colour})
        worksheet.conditional_format(75, 1, 75, 2, {"type": "no_blanks", "format": format_new_colour})

        worksheet.insert_image("B2", "Exponential.png")
        worksheet.hide_gridlines(2)
        worksheet.set_zoom(100)
        worksheet.set_column("A:A", 1)
        worksheet.set_column("B:B", 51)
        worksheet.set_column("C:C", 255)

    def main(self):
        self.config.common_columns_summary()
        self.reading_def()
        self.writing_definition()
        self.format_definition()
        self.logger.info('EOC for IO - {} Created'.format(self.config.ioid))


if __name__ == "__main__":
    pass
