from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType

from main import students_per_course

test_schema = StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
    StructField("course", StringType()),
    StructField("activation_date", StringType())
])


def test_machine_learning_students(spark_session):
    data_frame = spark_session.createDataFrame([
        [1, "Diana", "Machine Learning", "2020-05"],
        [2, "Jose", "Machine Learning", "2020-04"],
        [3, "Sebastian", "Engineering", "2020-05"]
    ], test_schema)

    machine_learning_students = students_per_course(data_frame, "Machine Learning").collect()

    assert len(machine_learning_students) == 2
    assert machine_learning_students[0]["name"] == "Diana"
    assert machine_learning_students[1]["name"] == "Jose"