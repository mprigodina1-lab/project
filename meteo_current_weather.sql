CREATE OR REPLACE PROCEDURE "meteo"."add_current_weather"("p_json_data" jsonb)
 AS $BODY$
BEGIN

	INSERT INTO meteo.current_weather (  
		"date", 
		"weather_id",
		"weather_main",
		"weather_description", 
		"weather_icon",
		"current_temp",
		"current_feels_like", 
		"current_pressure", 
		"current_humidity", 
		"current_visibility", 
		"current_wind_speed",
	  "current_wind_gust",
		"current_wind_deg", 
		"current_clouds",
		"current_rain_1h", 
		"current_snow_1h", 
		"current_sunrise", 
		"current_sunset"
-- 		"current_uvi", 
-- 		"current_dew_point"
    )

  SELECT 
    to_timestamp((t.val::json->> 'dt')::int4),
		
    (t.val::json->'weather' -> 0 ->> 'id')::int4,
		 t.val::json->'weather' -> 0 ->> 'main',
		 t.val::json->'weather' -> 0 ->> 'description',
		 t.val::json->'weather' -> 0 ->> 'icon',
		
    ((t.val::json -> 'main')::json ->> 'temp')::DECIMAL(6,2),
    ((t.val::json -> 'main')::json ->> 'feels_like')::DECIMAL(6,2),
    ((t.val::json -> 'main')::json ->> 'pressure')::DECIMAL(7,2),
    ((t.val::json -> 'main')::json ->> 'humidity')::DECIMAL(6,2),
    
    (t.val::json ->> 'visibility')::int4,
    
    (t.val::json -> 'wind' ->> 'speed')::DECIMAL(6,2),
    (t.val::json -> 'wind' ->> 'gust')::DECIMAL(6,2),
    (t.val::json -> 'wind' ->> 'deg')::DECIMAL(6,2),
    
    (t.val::json -> 'clouds' ->> 'all')::DECIMAL(6,2),
    
    (t.val::json -> 'rain' ->> '1h')::DECIMAL(6,2),
    
    (t.val::json -> 'snow' ->> '1h')::DECIMAL(6,2),
    
    to_timestamp((t.val::json -> 'sys' ->>'sunrise')::int4),
    to_timestamp((t.val::json -> 'sys' ->>'sunset')::int4)


	FROM  (VALUES(p_json_data)) AS t(val)
;
		
END;
		
$BODY$
  LANGUAGE plpgsql SECURITY DEFINER
