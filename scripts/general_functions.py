########################################
##Read S3 file
########################################
def readS3file(glueContext, list_path_tables, tables_format):
    """
    Load S3 files to the same dynamic data frame

    input:
    - glueContext: spark context created by GlueContext(SparkContext.getOrCreate())
    - list_path_tables (list): list with all the paths to all the tables we want to load in the same result table
    - tables_format (string): string with the format of the files to be loaded. i.e. "parquet" or "json"

    output:
    - DataFrame with all the files loaded
    """

    df = glueContext.create_dynamic_frame.from_options(
        connection_type="s3", connection_options={"paths": list_path_tables}, format=tables_format
    )
    return df


########################################
##Flatten json format
########################################
def flatten_struct(schema, prefix=""):
    """
    Helps to transform the schema of a spark DataFrame by selecting as columns all variables inside struct variables. Example of use for a df dataframe: df.select(flatten_struct(df.schema))

    input:
    - schema: DataFrame schema of the table
    - prefix (string): Don't use. This variable is only used when is called recursive by the function

    ouput:
    - List with the names of the variables to select from the table

    """

    from pyspark.sql.functions import col
    from pyspark.sql.types import StructType

    result = []
    for elem in schema:
        if isinstance(elem.dataType, StructType):
            result += flatten_struct(elem.dataType, prefix + elem.name + ".")
        else:
            result.append(col(prefix + elem.name).alias(prefix + elem.name))
    return result
