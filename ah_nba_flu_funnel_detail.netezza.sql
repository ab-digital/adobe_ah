DROP TABLE DST.AH_nba_flu_funnel_detail;
CREATE TABLE DST.AH_nba_flu_funnel_detail
(
CAMPAIGN                             CHARACTER VARYING(200),
COHORT                               CHARACTER VARYING(200),
EXPERIMENT_GROUP_TYPE                CHARACTER VARYING(200),
PROXY_ID                             CHARACTER VARYING(200),
FUNNEL_ENTRY_DT                      TIMESTAMP, 
LEVEL_NBR                            INTEGER,
LEVEL_DESC                           CHARACTER VARYING(200),
LEVEL_MIN_DT                         TIMESTAMP,
LEVEL_MAX_DT                         TIMESTAMP,
LEVEL_CNT                            INTEGER,
LEVEL_EVENT_ARRAY                    CHARACTER VARYING(200)
)
DISTRIBUTE ON RANDOM;

