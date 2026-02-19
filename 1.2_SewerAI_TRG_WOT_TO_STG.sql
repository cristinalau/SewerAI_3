CREATE OR REPLACE TRIGGER TRG_WOT_TO_STG
AFTER INSERT ON MNT.WORKORDERTASK
FOR EACH ROW
DECLARE
  c_site_oi CONSTANT NUMBER := 58;
BEGIN
  --------------------------------------------------------------------------
  -- CUSTOMERDATA.EPSEWERAI_WOT_STG (per task; keyed by TASK_UUID)
  -- Safe in row-level trigger: only queries MNT.WORKORDERS (not WORKORDERTASK)
  --------------------------------------------------------------------------
  MERGE INTO CUSTOMERDATA.EPSEWERAI_WOT_STG s
  USING (
    SELECT
      wo.WONUMBER                                        AS wonumber,
      wo.UUID                                            AS workorder_uuid,
      :NEW.UUID                                          AS task_uuid,
      :NEW.TASKNUMBER                                    AS tasknumber,
      NVL(:NEW.WOTASKTITLE,        wo.TITLE)             AS wotasktitle,
      NVL(:NEW.PLNDCOMPDATE_DTTM,  wo.PLANNEDCOMPL_DTTM) AS plndcompdate_dttm,
      NVL(:NEW.PLNDSTRTDATE_DTTM,  wo.PLNDSTRTDATE_DTTM) AS plndstrtdate_dttm,
      CASE
        WHEN :NEW.WORKCLASSIFI_OI IN (209, 211, 215, 266) THEN :NEW.WORKCLASSIFI_OI
        ELSE 209
      END                                                AS workclassifi_oi
    FROM MNT.WORKORDERS wo
    WHERE wo.WORKORDERSOI = :NEW.WORKORDER_OI
      AND wo.SITE_OI      = c_site_oi
  ) src
  ON (s.TASK_UUID = src.TASK_UUID)

  WHEN MATCHED THEN
    UPDATE SET
      s.WORKORDER_UUID     = src.WORKORDER_UUID,
      s.WONUMBER           = src.WONUMBER,
      s.TASKNUMBER         = src.TASKNUMBER,
      s.WOTASKTITLE        = src.WOTASKTITLE,
      s.PLNDCOMPDATE_DTTM  = src.PLNDCOMPDATE_DTTM,
      s.PLNDSTRTDATE_DTTM  = src.PLNDSTRTDATE_DTTM,
      s.WORKCLASSIFI_OI    = src.WORKCLASSIFI_OI,
      s.FEED_STATUS        = 'UPDATED'
    WHERE
          NVL(s.WONUMBER, '~') <> NVL(src.WONUMBER, '~')
       OR NVL(s.TASKNUMBER, -1) <> NVL(src.TASKNUMBER, -1)
       OR NVL(s.WOTASKTITLE, '~') <> NVL(src.WOTASKTITLE, '~')
       OR NVL(s.PLNDCOMPDATE_DTTM, DATE '1900-01-01') <> NVL(src.PLNDCOMPDATE_DTTM, DATE '1900-01-01')
       OR NVL(s.PLNDSTRTDATE_DTTM, DATE '1900-01-01') <> NVL(src.PLNDSTRTDATE_DTTM, DATE '1900-01-01')
       OR NVL(s.WORKCLASSIFI_OI, -1) <> NVL(src.WORKCLASSIFI_OI, -1)

  WHEN NOT MATCHED THEN
    INSERT (
      TASK_UUID,
      WORKORDER_UUID,
      WONUMBER,
      TASKNUMBER,
      WOTASKTITLE,
      PLNDCOMPDATE_DTTM,
      PLNDSTRTDATE_DTTM,
      WORKCLASSIFI_OI,
      FEED_STATUS
    )
    VALUES (
      src.TASK_UUID,
      src.WORKORDER_UUID,
      src.WONUMBER,
      src.TASKNUMBER,
      src.WOTASKTITLE,
      src.PLNDCOMPDATE_DTTM,
      src.PLNDSTRTDATE_DTTM,
      src.WORKCLASSIFI_OI,
      'NEW'
    );
END TRG_WOT_TO_STG;
