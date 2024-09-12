from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, FloatType

# Define a UDF that squares a number
@udf(returnType=FloatType())  # Use IntegerType for integer results
def square_number(s):
    return s * s
