-- GA and Firebase raw data exported to BigQuery is contained in a meta-table containing some nested repeated fields

-- A meta-table is a structure that is composed by many sub-tables. The name of the sub-tables usually differs only by a date (in format %Y%m%d) suffix.
-- Meta-tables can be selected with
SELECT `my-project.my_dataset.ga_sessions_*` ... ;
-- But can only be updated separately. In other words a statement like
UPDATE `my-project.my_dataset.ga_sessions_*` ... ;
-- Will result in error "Illegal operation (write) on meta-table"

DECLARE i INT64 DEFAULT 0;
DECLARE DATES ARRAY<DATE>;
DECLARE event_date STRING;
  
SET DATES = GENERATE_DATE_ARRAY(DATE(2022,1,22), DATE(2022, 08, 21), INTERVAL 1 DAY);

LOOP
    SET i = i + 1;  

    IF i > ARRAY_LENGTH(DATES) THEN 
      LEAVE; 
    END IF;

    SET event_date = FORMAT_DA('%Y%m%d',DATES[ORDINAL(i)]);

    execute immediate 'update table set field="value" where true';
  
END LOOP;

-- Nested repeated fields are records contained within a parent record with cardinality 0-n.
-- Nested repeated fields can't directly be accessed by an UPDATE statement.
-- Running a statement like
UPDATE `my-project.my_dataset.ga_sessions_20220801` SET hits.sourcePropertyInfo.sourcePropertyTrackingId = 'some value' ... ;
-- Will result in error "Cannot access field sourcePropertyInfo on a value with type ARRAY<STRUCT<..."
-- It is necessary update the nested field as a whole

UPDATE `my-project.my_dataset.ga_sessions_20220801`
SET hits = 
  (SELECT ARRAY_AGG(t)
  FROM (
    SELECT hit.* replace(
      STRUCT(
        CASE
          WHEN hit.sourcePropertyInfo.sourcePropertyDisplayName = "some value" THEN "some other value"
          ELSE hit.sourcePropertyInfo.sourcePropertyDisplayName
        END AS sourcePropertyDisplayName,
        hit.sourcePropertyInfo.sourcePropertyTrackingId )
      AS sourcePropertyInfo )
    FROM UNNEST(hits) AS hit )
  AS t )
WHERE true;

--------------

DECLARE i INT64 DEFAULT 0;
DECLARE DATES ARRAY<DATE>;
DECLARE event_date STRING;
declare query STRING;
  
SET DATES = GENERATE_DATE_ARRAY(DATE(2022,1,22), DATE(2022, 08, 21), INTERVAL 1 DAY);

LOOP
    SET i = i + 1;  

    IF i > ARRAY_LENGTH(DATES) THEN 
      LEAVE; 
    END IF;

    SET event_date = FORMAT_DA('%Y%m%d',DATES[ORDINAL(i)]);

    SET query = 'update `roig-global-web.181797792.ga_sessions_' || event_date || '` set hits = (select array_agg(t) from ( select hit.* replace( struct( case when hit.sourcePropertyInfo.sourcePropertyDisplayName= "Israel- Web (2)" then "Israel - Web" when hit.sourcePropertyInfo.sourcePropertyDisplayName= "Mexico -Web" then "Mexico - Web" else hit.sourcePropertyInfo.sourcePropertyDisplayName end as sourcePropertyDisplayName, hit.sourcePropertyInfo.sourcePropertyTrackingId ) as sourcePropertyInfo ) from unnest(hits) as hit ) as t ) where true';

    execute immediate query;
    select event_date || ' complete';
  
END LOOP;
