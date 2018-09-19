#coding=utf-8
#!/usr/bin/env python
"""
This is main class running for all scripts
"""
import Eoc_Summary
import Eoc_Daily
import Eoc_Video
import Eoc_Intraction
import EOC_definition
import sys
import SQLScript
import Config


if __name__ == '__main__':

    END_DATE = sys.argv[1]
    IO_ID = int(sys.argv[2])
    START_DATE = sys.argv[3]
    #FILE_PATH = sys.argv[4]
    #c = Config(END_DATE, IO_ID, START_DATE, FILE_PATH)
    c = Config(END_DATE, IO_ID, START_DATE)
    obj_sql = SQLScript.SqlScript(c)
    obj_sql.main()
    obj_summary = Eoc_Summary.Summary(c, obj_sql)
    obj_summary.main()
    obj_daily = Eoc_Daily.Daily(c, obj_sql)
    obj_daily.main()
    obj_Video = Eoc_Video.Video(c, obj_sql)
    obj_Video.main()
    obj_Interaction = Eoc_Intraction.Intraction(c, obj_sql)
    obj_Interaction.main()
    obj_Definition = EOC_definition.Definition(c)
    obj_Definition.main()
    c.saveAndCloseWriter()
    #o = Eoc_Summary.Summary
