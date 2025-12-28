CREATE TABLE "meteo"."weather_legend" (
  "id" int4 NOT NULL,
  "group" int4,
  "main" varchar COLLATE "pg_catalog"."default",
  "description" varchar COLLATE "pg_catalog"."default",
  "icon_day" varchar COLLATE "pg_catalog"."default",
  "icon_night" varchar COLLATE "pg_catalog"."default",
  CONSTRAINT "weather_pkey" PRIMARY KEY ("id")
)
;

ALTER TABLE "meteo"."weather" 
  OWNER TO "postgres";

COMMENT ON COLUMN "meteo"."weather"."id" IS 'Идентификатор';
COMMENT ON COLUMN "meteo"."weather"."group" IS 'Группа';
COMMENT ON COLUMN "meteo"."weather"."main" IS 'Наименование';
COMMENT ON COLUMN "meteo"."weather"."description" IS 'Описание';
COMMENT ON COLUMN "meteo"."weather"."icon_day" IS 'Иконка дневная';
COMMENT ON COLUMN "meteo"."weather"."icon_night" IS 'Иконка ночная';
