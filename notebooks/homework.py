import sys
from pathlib import Path
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
# import col, count, avg, concat_ws, collect_list, expr, split, first, row_number
from pyspark.sql.window import Window


SRC_DIR = './work/in/'
TRG_DIR = './work/out/'
SRC_CRIMES_FILENAME = 'crime.csv'
SRC_OFF_CODES_FILENAME = 'offense_codes.csv'
TRG_MART_FILENAME = 'crimes_stat_mart'


def build_crime_mart(src_dir, trg_dir):
    session = SparkSession.builder.appName('build_crime_stat_mart').getOrCreate()

    df_crimes = session.read.csv(Path(src_dir, SRC_CRIMES_FILENAME).as_posix(), header=True, inferSchema=True)
    df_codes = session.read.csv(Path(src_dir, SRC_OFF_CODES_FILENAME).as_posix(), header=True, inferSchema=True)

    # убираем дубликаты кодов и создаем колонку с коротким именем (первая часть до дефиса)
    df_codes = df_codes.dropDuplicates(
        ['CODE']
    ).withColumn(
        'offense_name', F.split(F.col('NAME'), ' - ').getItem(0)
    ).withColumn(
        'offense_name', F.trim(F.col('offense_name'))
    )
    # в основном датафрейме убираем строки с пустыми полями DISTRICT, Lat, Long
    df_crimes = df_crimes.dropDuplicates().filter(
        F.col('DISTRICT').isNotNull()
        & F.col("Lat").isNotNull()
        & F.col("Long").isNotNull()
    )

    # СТРОИМ ВИТРИНУ
    df = df_crimes.join(df_codes, df_crimes['OFFENSE_CODE'] == df_codes['CODE'], 'inner')
    df.createOrReplaceTempView("mart")
    df_mart = session.sql("""
        WITH 
            total AS (
                SELECT 
                    DISTRICT
                ,   COUNT(INCIDENT_NUMBER) as incidents
                ,   avg(Lat) AS avg_lat
                ,   avg(Long) AS avg_long 
                FROM mart m 
                GROUP BY DISTRICT
            )
        ,   top_crimes AS (
                SELECT 
                    DISTRICT
                ,   concat_ws(', ', collect_list(OFFENSE_NAME)) AS OFFENSE_NAMES
                FROM (
                    SELECT 
                        DISTRICT, OFFENSE_NAME, incidents,
                        row_number() over (partition by DISTRICT order by incidents DESC) as freq_range
                    FROM (
                        SELECT DISTRICT, OFFENSE_NAME, count(INCIDENT_NUMBER) as incidents 
                        FROM mart m GROUP BY DISTRICT, OFFENSE_NAME
                    ) t
                    ORDER BY DISTRICT, freq_range DESC
                )
                WHERE freq_range <= 3
                GROUP BY DISTRICT
            )
        ,   crimes_by_month as (
                SELECT 
                    DISTRICT
                ,   percentile_approx(incidents, 0.5) AS median
                FROM (
                    SELECT DISTRICT, date_trunc('MONTH', OCCURRED_ON_DATE) as mon, count(INCIDENT_NUMBER) as incidents 
                    FROM mart m 
                    GROUP BY DISTRICT, date_trunc('MONTH', OCCURRED_ON_DATE))
                GROUP BY DISTRICT
            )
        SELECT
            total.DISTRICT
        ,   total.incidents as crimes_total                   -- общее количество преступлений в этом районе
        ,   crimes_by_month.median as crimes_monthly          -- медиана числа преступлений в месяц в этом районе
        ,   top_crimes.OFFENSE_NAMES as frequent_crime_types  -- три самых частых crime_type за всю историю наблюдений в этом районе
        ,   total.avg_lat as lat                              -- широта координаты района, рассчитанная как среднее по всем широтам инцидентов;
        ,   total.avg_long  as lng                            -- долгота координаты района, рассчитанная как среднее по всем долготам инцидентов
        FROM total
            JOIN top_crimes ON  top_crimes.DISTRICT = total.DISTRICT
            JOIN crimes_by_month ON crimes_by_month.DISTRICT = total.DISTRICT
    """)

    # сохраняем витрину
    df_mart.write.parquet(Path(trg_dir, TRG_MART_FILENAME).as_posix(), mode="overwrite")

    session.stop()


if __name__ == "__main__":
    import sys
    build_crime_mart(src_dir=sys.argv[1], trg_dir=sys.argv[2])
    # build_crime_mart(src_dir=SRC_DIR, trg_dir=TRG_DIR)
