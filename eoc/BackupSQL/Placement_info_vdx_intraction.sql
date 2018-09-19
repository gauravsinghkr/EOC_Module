SELECT SUBSTR(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) AS PLACEMENT#,PRODUCT,sum(VWR_MUTE) as Vwrmute,sum(VWR_UNMUTE) as Vwrunmute,sum(VWR_PAUSE) as Vwrpause, 
sum(VWR_REWIND) as Vwrrewind, sum(VWR_RESUME) as Vwrresume,sum(VWR_REPLAY) as Vwrreplay, 
sum(VWR_FULL_SCREEN) as Vwrfullscreen,sum(ENG_MUTE) as Engmute, sum(ENG_UNMUTE) as Engunmute, 
sum(ENG_PAUSE) as Engpause, sum(ENG_REWIND) as Engrewind,sum(ENG_RESUME) as Engresume, sum(ENG_REPLAY) as Engreplay,
sum(ENG_FULL_SCREEN) as Engfullscreen, sum(DPE_MUTE) as Dpemute,sum(DPE_UNMUTE) as Dpeunmute, sum(DPE_PAUSE) as Dpepause, 
sum(DPE_REWIND) as Dperewind, sum(DPE_RESUME) as Dperesume,sum(DPE_REPLAY) as Dpereplay,sum(DPE_FULL_SCREEN) as Dpefullscreen 
FROM TFR_REP.EOC_VIDEO_DETAIL_VIEW WHERE IO_ID = {0} AND TO_CHAR(TO_DATE(DAY_DESC, 'MM/DD/YYYY'),'YYYY-MM-DD') BETWEEN '{1}' AND '{2}' GROUP BY PRODUCT,PLACEMENT_DESC ORDER BY PRODUCT