### Импорт библиотек 


```python
import io
import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
```
Этот код определяет функцию sparksession(), которая создает и возвращает объект SparkSession. SparkSession представляет точку входа для работ с данными в Apache Spark и обеспечивает доступ к различным функциям Spark. Созданный объект SparkSession имеет имя "SparkJob5-" с добавленным уникальным идентификатором, сгенерированным с использованием модуля uuid.
### Создаем spark сессию


```python
def _spark_session():
    return (SparkSession.builder
            .appName("SparkJob5-" + uuid.uuid4().hex)
            .getOrCreate())
```


```python
spark = _spark_session()
```

    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
    24/02/13 17:30:02 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable


## spark датафреймы из parquet файлов


```python
lineitem_df = spark.read.parquet('/Users/denis/Desktop/de/lineitem')
orders_df = spark.read.parquet('/Users/denis/Desktop/de/orders')
nation_df = spark.read.parquet('/Users/denis/Desktop/de/nation')
customer_df = spark.read.parquet('/Users/denis/Desktop/de/customer')
region_df = spark.read.parquet('/Users/denis/Desktop/de/region')
supplier_df = spark.read.parquet('/Users/denis/Desktop/de/supplier')
part_df = spark.read.parquet('/Users/denis/Desktop/de/part')
partsupp_df = spark.read.parquet('/Users/denis/Desktop/de/partsupp')
```

### Агрегация данных Spark

### Обработка данных для отчета по LineItems

Необходимо построить отчет по данным о позициях в заказе (lineitems) содержащий сводную информацию по позициям каждого заказа, когда либо совершенного в системе, группировать данные необходимо по идентификатору заказа.
Используйте сортировку по ключу заказа на возрастание.

| Поле            | Тип    | Описание                                                                                                              |
| --------------- | ------ | --------------------------------------------------------------------------------------------------------------------- |
| L_ORDERKEY      | bigint | уникальный идентификатор заказа                                                                                       |
| count           | int    | число позиций/вещей в заказе                                                                                          |
| sum_extendprice | float  | сумма заказа (сумма всех позиций по L_EXTENDEDPRICE)                                                                  |
| mean_discount   | float  | медиана скидок по позициям (L_DISCOUNT)                                                                               |
| mean_tax        | float  | средний налог в заказе между позициями                                                                                |
| delivery_days   | float  | среднее время доставки всех позиций заказа (в днях) с момента заказа (L_RECEIPTDATE) до момента доставки (L_SHIPDATE) |
| A_return_flags  | int    | количество заказов с флагом L_RETURNFLAG == 'A'                                                                       |
| R_return_flags  | int    | количество заказов с флагом L_RETURNFLAG == 'R'                                                                       |
| N_return_flags  | int    | количество заказов с флагом L_RETURNFLAG == 'N'                                                                       |


```python
lineitem_df \
.select(['L_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 
         'L_TAX', 'L_SHIPDATE', 'L_RECEIPTDATE', 'L_RETURNFLAG'])\
.groupby(['L_ORDERKEY'])\
.agg(F.count('L_ORDERKEY').alias("count"),
     F.sum('L_EXTENDEDPRICE').alias("sum_extendprice"),
     F.mean('L_DISCOUNT').alias("mean_discount"),
     F.mean('L_TAX').alias("mean_tax"),
     F.avg(F.datediff(F.col('L_RECEIPTDATE'), F.col('L_SHIPDATE'))).alias("delivery_days"),
     F.sum(F.when(F.col('L_RETURNFLAG')=='A', 1).otherwise(0)).alias("A_return_flags"),
     F.sum(F.when(F.col('L_RETURNFLAG')=='R', 1).otherwise(0)).alias("R_return_flags"),
     F.sum(F.when(F.col('L_RETURNFLAG')=='N', 1).otherwise(0)).alias("N_return_flags"))\
     .orderBy(['L_ORDERKEY']).show()
```

                                                                                    

    +----------+-----+------------------+--------------------+--------------------+------------------+--------------+--------------+--------------+
    |L_ORDERKEY|count|   sum_extendprice|       mean_discount|            mean_tax|     delivery_days|A_return_flags|R_return_flags|N_return_flags|
    +----------+-----+------------------+--------------------+--------------------+------------------+--------------+--------------+--------------+
    |         1|    6|         195298.34| 0.08166666666666668| 0.03666666666666667| 8.333333333333334|             0|             0|             6|
    |         2|    1|          63066.32|                 0.0|                0.05|               5.0|             0|             0|             1|
    |         3|    6|         287551.64| 0.06166666666666667|0.024999999999999998|15.833333333333334|             3|             3|             0|
    |         4|    1|           39819.0|                0.03|                0.08|               8.0|             0|             0|             1|
    |         5|    3|          125431.3| 0.05666666666666667|0.049999999999999996|13.666666666666666|             1|             2|             0|
    |         6|    1|          53697.73|                0.08|                0.03|               5.0|             1|             0|             0|
    |         7|    7|         298268.24| 0.06571428571428571|                0.04|15.714285714285714|             0|             0|             7|
    |        32|    6|130065.55999999998|0.056666666666666664| 0.03666666666666667|13.833333333333334|             0|             0|             6|
    |        33|    4|         132757.38|              0.0625|                0.03|             11.25|             3|             1|             0|
    |        34|    3|          54144.49| 0.03333333333333333| 0.06333333333333334| 7.666666666666667|             0|             0|             3|
    |        35|    6|         222234.81| 0.05166666666666667|0.041666666666666664|              18.5|             0|             0|             6|
    |        36|    1|          74029.62|                0.09|                 0.0|              20.0|             0|             0|             1|
    |        37|    3|         163678.74| 0.06333333333333334|0.043333333333333335|24.666666666666668|             3|             0|             0|
    |        38|    1|           63681.2|                0.04|                0.02|               1.0|             0|             0|             1|
    |        39|    6|         333812.06| 0.06166666666666667| 0.04833333333333333|19.833333333333332|             0|             0|             6|
    |        64|    1|          30837.66|                0.05|                0.02|              26.0|             0|             1|             0|
    |        65|    3|         116529.35|                0.04| 0.05000000000000001|16.666666666666668|             1|             0|             2|
    |        66|    2|         116587.52|                0.02| 0.07500000000000001|              13.0|             1|             1|             0|
    |        67|    6|         142813.22|                0.06| 0.05666666666666667|              12.5|             0|             0|             6|
    |        68|    7|         351237.36|0.049999999999999996|0.047142857142857146|10.285714285714286|             0|             0|             7|
    +----------+-----+------------------+--------------------+--------------------+------------------+--------------+--------------+--------------+
    only showing top 20 rows
    


### Обработка данных для отчета по Orders

Необходимо построить отчет по данным о заказах (orders) содержащий сводную информацию по заказам в разрезе месяца, страны откуда был отправлен заказ, а так же приоритета выполняемого заказа. 
Используйте сортировку по названию страны и приоритета заказа на возрастание.

### JOIN SPARK


```python
rdf = orders_df.join(customer_df, orders_df.O_CUSTKEY == customer_df.C_CUSTKEY, how = 'left')
rdf = rdf.join(nation_df, rdf.C_NATIONKEY == nation_df.N_NATIONKEY, how = 'left')
```


```python
rdf \
.select(['O_ORDERDATE', 'N_NAME', 'O_ORDERPRIORITY', 
         'O_ORDERKEY', 'O_TOTALPRICE', 'O_ORDERSTATUS']) \
.withColumn("O_MONTH",F.date_format(F.to_date("O_ORDERDATE", "yyyy-MM-dd"), "yyyy-MM")) \
.groupby(['O_MONTH', 'N_NAME', 'O_ORDERPRIORITY']) \
.agg(F.count('O_ORDERKEY').alias("orders_count"),
     F.avg('O_TOTALPRICE').alias("avg_order_price"),
     F.sum('O_TOTALPRICE').alias("sum_order_price"),
     F.min('O_TOTALPRICE').alias("min_order_price"), 
     F.max('O_TOTALPRICE').alias("max_order_price"), 
     F.sum(F.when(F.col('O_ORDERSTATUS')=='F', 1).otherwise(0)).alias("f_return_flags"),
     F.sum(F.when(F.col('O_ORDERSTATUS') == 'O', 1).otherwise(0)).alias("o_order_status"),
     F.sum(F.when(F.col('O_ORDERSTATUS') == 'P', 1).otherwise(0)).alias("p_order_status")
    ).orderBy(['N_NAME', 'O_ORDERPRIORITY']).show()
```

    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:33 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:34 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:34 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:34 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:34 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:34 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:34 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:34 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:34 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/12 23:20:35 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
                                                                                    

    +-------+-------+---------------+------------+------------------+--------------------+---------------+---------------+--------------+--------------+--------------+
    |O_MONTH| N_NAME|O_ORDERPRIORITY|orders_count|   avg_order_price|     sum_order_price|min_order_price|max_order_price|f_return_flags|o_order_status|p_order_status|
    +-------+-------+---------------+------------+------------------+--------------------+---------------+---------------+--------------+--------------+--------------+
    |1996-04|ALGERIA|       1-URGENT|        1469|152267.07248468342|2.2368032947999996E8|        1043.52|      469164.85|             0|          1469|             0|
    |1992-04|ALGERIA|       1-URGENT|        1512|149576.27086640213|2.2615932155000004E8|         979.39|      443304.14|          1512|             0|             0|
    |1997-08|ALGERIA|       1-URGENT|        1524|149620.19489501312|      2.2802117702E8|        1200.14|      413478.84|             0|          1524|             0|
    |1994-10|ALGERIA|       1-URGENT|        1538|152604.78979843954|      2.3470616671E8|        1059.17|      458074.71|          1538|             0|             0|
    |1995-05|ALGERIA|       1-URGENT|        1572|149716.88532442745|2.3535494372999996E8|        1035.78|      412887.06|            82|           607|           883|
    |1997-05|ALGERIA|       1-URGENT|        1552|150545.82843427837|2.3364712573000002E8|        1059.51|       427875.7|             0|          1552|             0|
    |1996-07|ALGERIA|       1-URGENT|        1555| 152636.0284823151|      2.3734902429E8|        1185.22|      432717.74|             0|          1555|             0|
    |1994-02|ALGERIA|       1-URGENT|        1476| 153808.7815040651|2.2702176150000006E8|        1109.39|      440522.12|          1476|             0|             0|
    |1998-05|ALGERIA|       1-URGENT|        1514|153842.94050858656|2.3291821193000007E8|        1191.87|       430828.1|             0|          1514|             0|
    |1995-09|ALGERIA|       1-URGENT|        1471|151302.75464989804|      2.2256635209E8|         889.75|      428619.66|             0|          1471|             0|
    |1993-07|ALGERIA|       1-URGENT|        1584| 151124.1548358586|      2.3938066126E8|         967.67|      448893.77|          1584|             0|             0|
    |1992-06|ALGERIA|       1-URGENT|        1473|147920.28797691784|2.1788658418999997E8|        1235.29|      397843.02|          1473|             0|             0|
    |1997-09|ALGERIA|       1-URGENT|        1473|150298.05248472502|2.2138903130999997E8|        1080.52|      400482.26|             0|          1473|             0|
    |1992-01|ALGERIA|       1-URGENT|        1563|150914.81793985923|      2.3587986044E8|         998.45|      416685.24|          1563|             0|             0|
    |1994-09|ALGERIA|       1-URGENT|        1486|152202.59374158815|2.2617305429999998E8|        1294.59|      489337.27|          1486|             0|             0|
    |1994-01|ALGERIA|       1-URGENT|        1500|150349.17458666666|      2.2552376188E8|        1080.05|      452929.02|          1500|             0|             0|
    |1998-04|ALGERIA|       1-URGENT|        1507| 150894.6789316523|2.2739828115000004E8|        1100.01|      427817.82|             0|          1507|             0|
    |1992-09|ALGERIA|       1-URGENT|        1466| 150874.2711255116|2.2118168146999997E8|        1159.47|      455772.02|          1466|             0|             0|
    |1993-11|ALGERIA|       1-URGENT|        1469|147758.32093260722|2.1705697345000002E8|         980.53|      465727.14|          1469|             0|             0|
    |1998-07|ALGERIA|       1-URGENT|        1508| 149344.9628912467|      2.2521220404E8|         1186.1|      427851.91|             0|          1508|             0|
    +-------+-------+---------------+------------+------------------+--------------------+---------------+---------------+--------------+--------------+--------------+
    only showing top 20 rows
    


### Обработка данных для отчета по Customers

Необходимо построить отчет по данным о клиентах (customers) содержащий сводную информацию по заказам в разрезе месяца, страны откуда был отправлен заказ, а так же приоритета выполняемого заказа. 
Используйте сортировку по названию страны (N_NAME) и приоритета заказа (C_MKTSEGMENT) на возрастание.

### JOIN SPARK


```python
rdf = region_df.join(nation_df, region_df.R_REGIONKEY == nation_df.N_REGIONKEY, how = 'left')
rdf = rdf.join(customer_df, rdf.N_NATIONKEY == customer_df.C_NATIONKEY, how = 'left')
```


```python
rdf.select(['R_NAME', 'N_NAME', 'C_MKTSEGMENT', 'C_CUSTKEY', 'C_ACCTBAL']) \
.groupby(['R_NAME', 'N_NAME', 'C_MKTSEGMENT']) \
.agg(F.countDistinct('C_CUSTKEY').alias("unique_customers_count"),
     F.avg('C_ACCTBAL').alias("avg_acctbal"),
     F.mean('C_ACCTBAL').alias("mean_acctbal"),
     F.min('C_ACCTBAL').alias("min_acctbal"),
     F.max('C_ACCTBAL').alias("max_acctbal")
    ).orderBy(['N_NAME', 'C_MKTSEGMENT']).show()
```

    24/02/12 23:49:49 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    [Stage 81:>                                                         (0 + 8) / 8]

    +-------+---------+------------+----------------------+------------------+------------------+-----------+-----------+
    | R_NAME|   N_NAME|C_MKTSEGMENT|unique_customers_count|       avg_acctbal|      mean_acctbal|min_acctbal|max_acctbal|
    +-------+---------+------------+----------------------+------------------+------------------+-----------+-----------+
    | AFRICA|  ALGERIA|  AUTOMOBILE|                 11890| 4489.457164003364| 4489.457164003364|    -999.35|    9998.88|
    | AFRICA|  ALGERIA|    BUILDING|                 12068| 4494.172725389461| 4494.172725389461|    -999.29|    9998.43|
    | AFRICA|  ALGERIA|   FURNITURE|                 11826| 4480.853280060884| 4480.853280060884|    -999.89|    9999.01|
    | AFRICA|  ALGERIA|   HOUSEHOLD|                 12142| 4489.274984351838| 4489.274984351838|    -998.98|    9998.86|
    | AFRICA|  ALGERIA|   MACHINERY|                 11990| 4471.501088407005| 4471.501088407005|    -999.46|    9998.69|
    |AMERICA|ARGENTINA|  AUTOMOBILE|                 11992| 4537.902103068711| 4537.902103068711|    -999.63|    9998.88|
    |AMERICA|ARGENTINA|    BUILDING|                 11990| 4429.626226855715| 4429.626226855715|    -999.95|    9999.82|
    |AMERICA|ARGENTINA|   FURNITURE|                 11825|4420.1204558139525|4420.1204558139525|    -999.71|    9998.11|
    |AMERICA|ARGENTINA|   HOUSEHOLD|                 11872| 4574.415051381398| 4574.415051381398|    -999.88|    9998.95|
    |AMERICA|ARGENTINA|   MACHINERY|                 12162| 4468.079357835883| 4468.079357835883|    -999.52|    9999.59|
    |AMERICA|   BRAZIL|  AUTOMOBILE|                 11930| 4474.693092204526| 4474.693092204526|    -999.68|    9998.31|
    |AMERICA|   BRAZIL|    BUILDING|                 11991| 4486.172500208489| 4486.172500208489|    -997.58|    9999.84|
    |AMERICA|   BRAZIL|   FURNITURE|                 12032| 4558.802776761965| 4558.802776761965|    -998.88|    9999.19|
    |AMERICA|   BRAZIL|   HOUSEHOLD|                 11990| 4495.771888240204| 4495.771888240204|    -999.03|    9998.47|
    |AMERICA|   BRAZIL|   MACHINERY|                 12009| 4489.320288949955| 4489.320288949955|    -999.26|     9998.5|
    |AMERICA|   CANADA|  AUTOMOBILE|                 11957| 4523.445320732625| 4523.445320732625|    -999.84|     9999.4|
    |AMERICA|   CANADA|    BUILDING|                 12076| 4527.305245114278| 4527.305245114278|    -999.65|    9999.84|
    |AMERICA|   CANADA|   FURNITURE|                 11860| 4491.722582630692| 4491.722582630692|    -999.37|    9999.49|
    |AMERICA|   CANADA|   HOUSEHOLD|                 11944|  4479.98520679839|  4479.98520679839|    -999.99|    9999.41|
    |AMERICA|   CANADA|   MACHINERY|                 12012| 4479.516843989343| 4479.516843989343|    -998.23|    9999.93|
    +-------+---------+------------+----------------------+------------------+------------------+-----------+-----------+
    only showing top 20 rows
    


                                                                                    

### Обработка данных для отчета по Suppliers

Необходимо построить отчет по данным о поставщиках(suppliers) содержащий сводную информацию в разрезе страны и региона поставщика. 
Используйте сортировку по названию страны (N_NAME) и региона (R_NAME) на возрастание.


```python
rdf = region_df.join(nation_df, region_df.R_REGIONKEY == nation_df.N_REGIONKEY, how = 'left')
rdf = rdf.join(supplier_df, rdf.N_NATIONKEY == supplier_df.S_NATIONKEY, how = 'left')
```


```python
rdf \
.select(['R_NAME', 'N_NAME', 'S_SUPPKEY', 'S_ACCTBAL']) \
.groupby(['R_NAME', 'N_NAME']) \
.agg(F.countDistinct('S_SUPPKEY').alias("unique_supplers_count"),
     F.avg('S_ACCTBAL').alias("avg_acctbal"),
     F.mean('S_ACCTBAL').alias("mean_acctbal"),
     F.min('S_ACCTBAL').alias("min_acctbal"),
     F.max('S_ACCTBAL').alias("max_acctbal")
    ).orderBy(['N_NAME', 'R_NAME']).show()
```

    +-----------+----------+---------------------+------------------+------------------+-----------+-----------+
    |     R_NAME|    N_NAME|unique_supplers_count|       avg_acctbal|      mean_acctbal|min_acctbal|max_acctbal|
    +-----------+----------+---------------------+------------------+------------------+-----------+-----------+
    |     AFRICA|   ALGERIA|                 3934| 4486.125091509919| 4486.125091509919|    -998.44|    9999.01|
    |    AMERICA| ARGENTINA|                 4007| 4487.078779635633| 4487.078779635633|    -995.61|    9998.69|
    |    AMERICA|    BRAZIL|                 3995| 4537.748092615765| 4537.748092615765|    -996.42|    9994.95|
    |    AMERICA|    CANADA|                 4054|  4574.45507153429|  4574.45507153429|    -997.13|    9999.93|
    |       ASIA|     CHINA|                 3988| 4508.844879638919| 4508.844879638919|    -997.43|    9999.57|
    |MIDDLE EAST|     EGYPT|                 3981| 4593.200592815871| 4593.200592815871|    -994.14|     9998.2|
    |     AFRICA|  ETHIOPIA|                 3945| 4516.879160963248| 4516.879160963248|    -998.16|    9997.98|
    |     EUROPE|    FRANCE|                 3961| 4567.553504165616| 4567.553504165616|    -993.76|    9998.48|
    |     EUROPE|   GERMANY|                 4049| 4461.351395406272| 4461.351395406272|    -998.37|    9994.37|
    |       ASIA|     INDIA|                 4079| 4413.585275802901| 4413.585275802901|    -996.93|    9999.06|
    |       ASIA| INDONESIA|                 3974| 4490.630294413685| 4490.630294413685|    -994.73|    9997.31|
    |MIDDLE EAST|      IRAN|                 4023| 4597.354407158833| 4597.354407158833|    -998.27|    9999.72|
    |MIDDLE EAST|      IRAQ|                 4095| 4504.413533577534| 4504.413533577534|     -999.2|    9999.21|
    |       ASIA|     JAPAN|                 4009| 4513.741701172358| 4513.741701172358|     -994.9|     9997.7|
    |MIDDLE EAST|    JORDAN|                 3933| 4578.277345537755| 4578.277345537755|    -998.22|    9999.58|
    |     AFRICA|     KENYA|                 4044| 4372.236041048461| 4372.236041048461|    -999.89|     9992.7|
    |     AFRICA|   MOROCCO|                 3990| 4557.683300751879| 4557.683300751879|    -998.69|    9995.38|
    |     AFRICA|MOZAMBIQUE|                 3924| 4529.804755351675| 4529.804755351675|    -997.74|    9997.48|
    |    AMERICA|      PERU|                 3991| 4565.430395890751| 4565.430395890751|    -997.61|    9999.01|
    |     EUROPE|   ROMANIA|                 4029|4451.3339215686265|4451.3339215686265|     -997.0|    9995.22|
    +-----------+----------+---------------------+------------------+------------------+-----------+-----------+
    only showing top 20 rows
    


### Обработка данных для отчета по Part

Необходимо построить отчет по данным о грузоперевозках (part) содержащий сводную информацию в разрезе страны поставки (N_NAME), типа поставки (P_TYPE) и типа контейнера (P_CONTAINER). 
Используйте сортировку по названию страны (N_NAME) , типа поставки (P_TYPE) и типа контейнера (P_CONTAINER) на возрастание.


```python
rdf = supplier_df.join(nation_df, supplier_df.S_NATIONKEY == nation_df.N_NATIONKEY, how = 'left')
rdf = rdf.join(partsupp_df, rdf.S_SUPPKEY == partsupp_df.PS_SUPPKEY, how = 'left')
rdf = rdf.join(part_df, rdf.PS_PARTKEY == part_df.P_PARTKEY, how = 'left')
```


```python
rdf = rdf \
.select(['N_NAME', 'P_TYPE', 'P_CONTAINER', 'PS_PARTKEY',
         'P_RETAILPRICE', 'P_SIZE', 'P_RETAILPRICE', 'PS_SUPPLYCOST']) \
.groupby(['N_NAME', 'P_TYPE', 'P_CONTAINER']) \
.agg(F.countDistinct('PS_PARTKEY').alias("parts_count"),
     F.avg('P_RETAILPRICE').alias("avg_retailprice"),
     F.sum('P_SIZE').alias("size"),
     F.mean('P_RETAILPRICE').alias("mean_retailprice"),
     F.min('P_RETAILPRICE').alias("min_retailprice"),
     F.max('P_RETAILPRICE').alias("max_retailprice"),
     F.avg('PS_SUPPLYCOST').alias("avg_supplycost"),
     F.mean('PS_SUPPLYCOST').alias("mean_supplycost"),
     F.min('PS_SUPPLYCOST').alias("min_supplycost"),
     F.max('PS_SUPPLYCOST').alias("max_supplycost")
).orderBy(['N_NAME', 'P_TYPE', 'P_CONTAINER'])
rdf.show()
```

    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:11 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:12 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:12 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:12 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:12 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:12 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:12 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:12 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:12 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:12 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:13 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:14 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:16 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:18 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:20 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:22 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:23 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:23 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:24 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:25 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:26 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:26 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:26 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:26 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:26 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:26 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:27 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    24/02/13 17:36:27 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
    [Stage 91:===================================================>      (8 + 1) / 9]

    +-------+--------------------+-----------+-----------+------------------+----+------------------+---------------+---------------+------------------+------------------+--------------+--------------+
    | N_NAME|              P_TYPE|P_CONTAINER|parts_count|   avg_retailprice|size|  mean_retailprice|min_retailprice|max_retailprice|    avg_supplycost|   mean_supplycost|min_supplycost|max_supplycost|
    +-------+--------------------+-----------+-----------+------------------+----+------------------+---------------+---------------+------------------+------------------+--------------+--------------+
    |ALGERIA|ECONOMY ANODIZED ...|  JUMBO BAG|         41|1449.0938636363635|1119|1449.0938636363635|         978.02|        1958.94|           485.485|           485.485|          4.43|         936.8|
    |ALGERIA|ECONOMY ANODIZED ...|  JUMBO BOX|         53| 1507.499649122807|1358| 1507.499649122807|         944.95|        2014.88| 508.1329824561403| 508.1329824561403|         25.15|        988.16|
    |ALGERIA|ECONOMY ANODIZED ...|  JUMBO CAN|         58| 1579.811746031746|1711| 1579.811746031746|        1056.05|        2032.95| 567.9615873015873| 567.9615873015873|         65.59|        970.84|
    |ALGERIA|ECONOMY ANODIZED ...| JUMBO CASE|         48| 1499.149411764706|1069| 1499.149411764706|         953.04|        2062.96|467.61372549019615|467.61372549019615|         12.14|        993.42|
    |ALGERIA|ECONOMY ANODIZED ...| JUMBO DRUM|         55|1567.2895081967213|1448|1567.2895081967213|        1077.15|        1967.85| 564.5232786885246| 564.5232786885246|         16.42|        988.82|
    |ALGERIA|ECONOMY ANODIZED ...|  JUMBO JAR|         47|         1498.4896|1109|         1498.4896|         955.03|        1939.87|          574.0848|          574.0848|         74.38|        997.37|
    |ALGERIA|ECONOMY ANODIZED ...| JUMBO PACK|         52|1491.0015789473684|1470|1491.0015789473684|         978.03|        2068.91| 507.5159649122807| 507.5159649122807|          4.81|        996.95|
    |ALGERIA|ECONOMY ANODIZED ...|  JUMBO PKG|         51|1449.4015384615382|1362|1449.4015384615382|         951.01|        2001.93|438.96538461538455|438.96538461538455|         33.03|         952.8|
    |ALGERIA|ECONOMY ANODIZED ...|     LG BAG|         59|1500.8648437499999|1791|1500.8648437499999|        1018.07|        2061.94|      508.34765625|      508.34765625|         14.93|        988.33|
    |ALGERIA|ECONOMY ANODIZED ...|     LG BOX|         46|1506.6987500000002|1199|1506.6987500000002|        1020.97|        2062.93| 537.9260416666667| 537.9260416666667|         16.51|        983.42|
    |ALGERIA|ECONOMY ANODIZED ...|     LG CAN|         48|1522.0828571428572|1271|1522.0828571428572|          922.0|        2027.91| 534.8712244897958| 534.8712244897958|         32.21|        991.32|
    |ALGERIA|ECONOMY ANODIZED ...|    LG CASE|         57| 1468.602711864407|1507| 1468.602711864407|         978.98|        2053.91|  549.025593220339|  549.025593220339|         39.96|        976.01|
    |ALGERIA|ECONOMY ANODIZED ...|    LG DRUM|         40|           1547.93|1085|           1547.93|         992.02|        2053.86| 495.5260975609756| 495.5260975609756|         38.07|        996.62|
    |ALGERIA|ECONOMY ANODIZED ...|     LG JAR|         61|1470.8933333333337|1734|1470.8933333333337|         965.02|        2006.94|494.04507936507935|494.04507936507935|         10.55|        999.26|
    |ALGERIA|ECONOMY ANODIZED ...|    LG PACK|         45| 1455.934347826087|1377| 1455.934347826087|         930.98|        2033.88| 530.6771739130435| 530.6771739130435|          8.78|        991.42|
    |ALGERIA|ECONOMY ANODIZED ...|     LG PKG|         37|1430.8174999999999| 920|1430.8174999999999|         965.98|        1969.85|         528.35725|         528.35725|         46.91|        980.83|
    |ALGERIA|ECONOMY ANODIZED ...|    MED BAG|         42|1429.0290909090907|1223|1429.0290909090907|        1010.07|        1959.81| 485.4259090909091| 485.4259090909091|          6.47|        971.52|
    |ALGERIA|ECONOMY ANODIZED ...|    MED BOX|         37|1495.9052631578945|1050|1495.9052631578945|         921.94|         1990.9| 468.3955263157894| 468.3955263157894|          4.92|        991.69|
    |ALGERIA|ECONOMY ANODIZED ...|    MED CAN|         51|1457.3981481481485|1436|1457.3981481481485|          978.0|        1953.92|471.80444444444447|471.80444444444447|          8.07|         996.8|
    |ALGERIA|ECONOMY ANODIZED ...|   MED CASE|         57|1375.9603278688523|1759|1375.9603278688523|          933.0|        1992.95|509.84098360655736|509.84098360655736|         10.99|        990.73|
    +-------+--------------------+-----------+-----------+------------------+----+------------------+---------------+---------------+------------------+------------------+--------------+--------------+
    only showing top 20 rows
    


                                                                                    
