from pyspark.sql.functions import col


def students_per_course(data_frame, course):
    return (data_frame.filter(col("course") == course))



