from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType


@udf(returnType=FloatType())
def square_number(s):
    """Define a spark UDF that squares a number

    Args:
        s (double): 数字

    Returns:
        double: 平方数
    """
    return s * s
