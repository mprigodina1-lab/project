CREATE OR REPLACE PROCEDURE "meteo"."add_forecast_hourly"(IN "p_json_data" json, INOUT "total_count" int4=0)
 AS $BODY$
BEGIN
	DELETE FROM "meteo"."forecast_hourly";
	
	INSERT INTO meteo.forecast_hourly (  
		date,
		hourly_temp,
		hourly_feels_like,
		hourly_pressure,
		hourly_humidity,
-- 		hourly_dew_point,
-- 		hourly_uvi,
		hourly_clouds,
		hourly_visibility,
		hourly_wind_speed,
		hourly_wind_gust,
		hourly_wind_deg,
		hourly_pop,
		hourly_rain_1h,
		hourly_snow_1h,
		weather_id,
		weather_main,
		weather_description,
		weather_icon )		
	SELECT 
		to_timestamp((jd.val ->> 'dt')::int4),
    
    (jd.val -> 'main' ->> 'temp')::DECIMAL(6, 2),
    (jd.val -> 'main' ->> 'feels_like')::DECIMAL(6, 2),
    (jd.val -> 'main' ->> 'pressure')::DECIMAL(6, 2),
    (jd.val -> 'main' ->> 'humidity')::DECIMAL(6, 2),

-- 		CAST(VALUE::json->>'dew_point' as NUMERIC),
-- 		CAST(VALUE::json->>'uvi' as NUMERIC),

     (jd.val -> 'clouds' ->> 'all')::DECIMAL(6, 2),
     (jd.val ->> 'visibility')::int2,
     
     (jd.val -> 'wind' ->> 'speed')::DECIMAL(6, 2),   
     (jd.val -> 'wind' ->> 'gust')::DECIMAL(6, 2),  
     (jd.val -> 'wind' ->> 'deg')::DECIMAL(6, 2),
     
     (jd.val ->> 'pop')::DECIMAL(6, 2),
     
     (jd.val -> 'rain' ->> '3h')::DECIMAL(6, 2),
     (jd.val -> 'snow' ->> '3h')::DECIMAL(6, 2),
     
     (jd.val -> 'weather' -> 0 ->> 'id')::int2,
     (jd.val -> 'weather' -> 0 ->> 'main')::VARCHAR,
     (jd.val -> 'weather' -> 0 ->> 'description')::VARCHAR,
     (jd.val -> 'weather' -> 0 ->> 'icon')::VARCHAR
	FROM (values (p_json_data::json)) as jf(val)
	CROSS JOIN LATERAL json_array_elements(jf.val->'list') as jd(val);
  
  GET DIAGNOSTICS total_count := ROW_COUNT;
	
END;
		
$BODY$
  LANGUAGE plpgsql SECURITY DEFINER
