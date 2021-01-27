# Dataset 1: Employee & Manager details.csv
# empNO|empName|mangNo|deprtmnt|salary
# 1|name1|man1|dep1|21000
# 2|name2|man1|dep1|22000
# 3|name3|man2|dep2|21000
# 4|name4|man2|dep3|21000
# 5|name5|man2|dep3|22000
# 6|name6|man2|dep3|22000

# Q1: Find the highest salaried employee from each department

# Dataset 2: Project details.csv
# projNo|projName|empNo
# 100|analytics|1
# 100|analytics|2
# 101|machine learning|3
# 101|machine learning|1
# 101|machine learning|4

# Q2: Find each employee in the form of list working on each project & manager.
#+------+------+---------+
#|projNo|mangNo|employees|
#+------+------+---------+
#|   101|  man2|   [3, 4]|
#|   101|  man1|      [1]|
#|   100|  man1|   [1, 2]|
#+------+------+---------+

# .\bin\spark-submit q1.py

from pyspark.sql.functions import collect_list,col
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("FindEmployeesAssignedToProject")\
        .getOrCreate()

    df = spark.read.csv(path=".\dataset\employee_details.csv",header=True, sep="|")
    df = df.withColumn('salary', col('salary').cast(IntegerType()))
    # Register df as temp table
    df.registerTempTable("emp_details")
    employees_sal = spark.sql("select deprtmnt, empNo, salary , DENSE_RANK() OVER (partition by deprtmnt ORDER BY salary DESC) RN From emp_details")
    employees_with_highest_sal = employees_sal.filter(col('RN')==1)

    # Read project details csv
    df2 = spark.read.csv(path=".\dataset\project_details.csv", header=True, sep="|")
    # Join both datasets
    df3 = df2.join(df, 'empNo', 'inner')
    # Final result
    df3.groupBy('projNo','mangNo').agg(collect_list('salary')).withColumnRenamed('collect_list(salary)', 'employees').show()

    spark.stop()

