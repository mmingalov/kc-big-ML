import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

#Создайте Pandas UDF функцию c именем card_number_mask которая должна будет частично скрывать номера банковских карт следующим образом:
# первый 4 цифры карточки не изменяются
# последние 4 цифры карточки не изменяются
# все остальные цифры заменяются символом 'X'
# Пример:
# 4034954837458345 => 4034XXXXXXXX8345

# Вам поможет:
# Документация pandas_udf
# Series.str.slice_replace

@pandas_udf(StringType())
def card_number_mask(s: pd.Series) -> pd.Series:
    ss = s.str.slice_replace(start=4,stop=12, repl='XXXXXXXX')
    return ss


if __name__ == "__main__":
    spark = SparkSession.builder.appName('PySparkUDF').getOrCreate()
    df = spark.createDataFrame([(1, "4042654376478743"), (2, "4042652276478747")], ["id", "card_number"])
    df.show()
    dfr = df.withColumn("hidden", card_number_mask("card_number"))
    dfr.show(truncate=False)
