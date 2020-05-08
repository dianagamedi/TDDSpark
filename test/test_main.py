from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType

from main import students_per_course

test_schema = StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
    StructField("course", StringType()),
    StructField("activation_date", StringType())
])


def test_return_only_students_enrolled_on_specific_course(spark_session):
    data_frame = spark_session.createDataFrame([
        [1, "Diana", "Machine Learning", "2020-05"],
        [2, "Jose", "Machine Learning", "2020-04"],
        [3, "Sebastian", "Engineering", "2020-05"]
    ], test_schema)

    students = students_per_course(data_frame, "Machine Learning").collect()

    assert len(students) == 2
    assert students[0]["name"] == "Diana"
    assert students[1]["name"] == "Jose"


def test_returns_zero_when_there_are_no_students_enrolled_on_a_course(spark_session):
    data_frame = spark_session.createDataFrame([
        [1, "Diana", "Machine Learning", "2020-05"],
        [2, "Jose", "Machine Learning", "2020-04"],
        [3, "Sebastian", "Engineering", "2020-05"]
    ], test_schema)

    students = students_per_course(data_frame, "Data Science").collect()

    assert len(students) == 0