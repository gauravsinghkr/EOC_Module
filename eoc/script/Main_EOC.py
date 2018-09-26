#coding=utf-8
#!/usr/bin/env python
"""
This is main class running for all scripts
"""

import sys
"""
import os
home = os.path.dirname(sys.argv[0])
sys.path.append(os.path.join(home, "EOC_Module/eoc/script"))
import Eoc_Summary
import Eoc_Daily
import Eoc_Video
import Eoc_Intraction
import EOC_definition
import SQLScript
from config import Config
"""
from EOC_Module.eoc.script import Eoc_Summary
from EOC_Module.eoc.script import Eoc_Daily
from EOC_Module.eoc.script import Eoc_Video
from EOC_Module.eoc.script import Eoc_Intraction
from EOC_Module.eoc.script import EOC_definition
from EOC_Module.eoc.script import SQLScript
from EOC_Module.eoc.script.config import Config


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
