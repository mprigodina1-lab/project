WITH me AS (SELECT MAX(cw1.date) AS max_date, MIN(cw1.date) FILTER (WHERE cw1.date::DATE = p_date_from) AS min_date FROM meteo.current_weather cw1)

-- Выводит 1 строку по переданной дате, по умолчанию - сегодня	
SELECT 
		cw.id,
		cw.date::TIMESTAMP,
		cw.weather_id,
		cw.weather_main,
		cw.weather_description,
		cw.weather_icon::VARCHAR,
		cw.current_temp::DECIMAL(6,2),
		cw.current_feels_like,
		CAST(cw.current_pressure / 1.333 AS decimal(4,0)) AS current_pressure,
		cw.current_humidity,
		cw.current_visibility,
		cw.current_wind_speed,
		cw.current_wind_deg,
			 
		CASE WHEN cw.current_wind_speed <> 0 THEN
	
		CASE WHEN cw.current_wind_deg BETWEEN 0 AND 22 OR cw.current_wind_deg BETWEEN 338 AND 360 THEN 'С'
	WHEN cw.current_wind_deg BETWEEN 23 AND 67   THEN 'СВ'
	WHEN cw.current_wind_deg BETWEEN 68 AND 112  THEN 'В'
	WHEN cw.current_wind_deg BETWEEN 113 AND 157 THEN 'ЮВ'
		WHEN cw.current_wind_deg BETWEEN 158 AND 202 THEN 'Ю'
		WHEN cw.current_wind_deg BETWEEN 203 AND 247 THEN 'ЮЗ'
		WHEN cw.current_wind_deg BETWEEN 247 AND 292 THEN 'З'
		WHEN cw.current_wind_deg BETWEEN 293 AND 337 THEN 'СВ'
		END
		
		ELSE 'Штиль'
		END::VARCHAR AS current_wind_desc,
		
		cw.current_clouds,
		cw.current_rain_1h,
		cw.current_snow_1h,
		cw.current_sunrise,
		cw.current_sunset,
		cw.current_uvi,
		cw.current_dew_point,
		cw.current_wind_gust
	FROM meteo.current_weather cw
