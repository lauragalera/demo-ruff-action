########################################
##Check minimum dataset size
########################################
def check_dataset_size(df, min_expected_size):
    """
    If the dataset of the is below the minimum expected size it returns a False
    Inputs:
    - df: dataframe we want to check the size
    - min_expected_size (int): The minimum number of rows df is expected to have
    Outputs:
    - False if df is below the expected minimum.
    """

    s = df.count()
    if s < min_expected_size:
        print(
            "WARNING!: migrated table has lower size ("
            + str(s)
            + ") than the minimum expected ("
            + str(min_expected_size)
            + ")!"
        )
        return False


########################################
##Check data types according to dictionary
########################################
def check_data_types(df, dictionary_selected, ignore=None):
    """
    This function returns the name of the columns that their type don't match with the type described in the input dictionary.

    input:
    - df: Spark DataFrame which we want to check data types
    - dictionary_selected: TODO
    - ignore: variables to ignore in this test (ex: ignore = ("var1", "var2")); default None

    output:
    - List with all variable that do not match with the input dictionary teeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeest
    """

    if ignore != None:
        df = df.drop(*ignore)

    wrong = []
    for i in df.dtypes:
        var_name = i[0]
        var_type = i[1]

        # Get expected data type from dictionary for every variable
        expected_data_type = dictionary_selected[dictionary_selected["final_name"] == var_name]["migration_type"].iloc[
            0
        ]
        if var_type != expected_data_type:
            wrong.append(var_name)
    return wrong


########################################
##Check missing data
########################################
def check_null_variables(df, cols):
    """
    This function returns the name of the columns that we want to be non nullable but they have null values inside.

    input:
    - check_df: Spark DataFrame which we want to check null values
    - cols (list): list of the column names we want to check if they don't have null values inside.

    output:
    - List with all variables that have null values
    """

    from pyspark.sql import functions as f

    # Select columns from dataframe and check if null/nan/none/'' , accounting for timestamp/date types
    df_issues = df.select(
        *[
            (
                # if not timestamp or date
                f.count(
                    f.when(
                        f.isnan(c)
                        | f.col(c).contains("NULL")
                        | (f.col(c) == "")
                        | f.col(c).isNull()
                        | f.col(c).contains("None"),
                        c,
                    )
                )
                if t not in ("timestamp", "date")
                # timestamp or date
                else f.count(
                    f.when(
                        f.col(c).contains("NULL") | (f.col(c) == "") | f.col(c).isNull() | f.col(c).contains("None"), c
                    )
                )
            ).alias(c)
            for c, t in df.dtypes
            if c in cols
        ]
    )

    df_issues.show()

    # Transform df_issues to a row list
    row_list = df_issues.collect()

    # Get columns with issues
    wrong = []
    for i in range(len(cols)):
        if row_list[0][i] != 0:
            wrong.append(cols[i])

    return wrong


########################################
##Check variable range
########################################
def check_variable_min_max_range(df, var_name, min_lim, max_lim):
    """
    This function checks if specific parameter is within given expected values

    input:
    - df (DataFrame): table to check
    - var_name (str): name of the variable to check
    - min_lim (int, double or str): minimum value accepted for this variable (e.g. for timestamps we'll use minimum: "2013-01-01", and maximum the current date set as <code>datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))</code> )
    - max_lim (int, double or str): maximum value accepted for this variable


    output:
    - df_wrong: DataFrame subset with all the wrong entries
    """

    from pyspark.sql.functions import col

    if var_name in df.columns:
        # Filter table if values out of the min-max expected values
        df_wrong = df.filter((col(var_name) < min_lim) | (col(var_name) > max_lim))

        # Warning if there're out of range entries
        if df_wrong.count() > 0:
            print("WARNING: parameter " + var_name + " out of range in: " + str(df_wrong.count()) + " entries!")

        return df_wrong

    else:
        print(var_name + " column is not in df")


########################################
##Check validations and put them in the log
########################################
def check_all_validations(
    essential_cols_not_in_df=[],
    check_data_types_result=[],
    check_dataset_size=None,
    check_null_vars_result=[],
    check_variable_formats=[],
    check_duplicates=None,
    check_data_consistency=[],
    try_catch_failed=None,
):
    """
    Takes as input all the validation performed and checks if all of them are passed. Otherwise, it creates a Notification with the non passed checks.

    Inputs:
        - essential_cols_not_in_df (list): list with all the essential variable which are not in the imported table. Essential variable is defined as those variable which have to be in the imported table, otherwise the imported information is incorrect
        - check_data_types_result (list): list with all variables which don't have the correct data type when imported.
        - check_dataset_size (boolean): if true, the imported dataset matches the expected dataset size.
        - check_null_vars_result (list): list with all variables which are not supposed to have null variables but they have.
        - check_variable_formats (list): list with all variable which don't have the expected format. i.e. a variable date is expected to have this format dd-mm-yyyy but it comes with mm-dd-yyyy
        - check_duplicates (boolean): if true, the imported dataset has no duplicates
        - check_data_consistency (list): list with all variable which don't have the expected value. i.e. the variable created_at of an event date imported from the file of yesterday events, is supposed to have all their values compressed within yesterday
        - try_catch_failed (string): if empty, there is not an execution fail. Otherwise, this variable will contain the error catched
    """

    import os

    # Assume all checks are true and if any of the checks is not passed then change it to False
    all_checks_ok = True

    print("Analyzing checks...")

    if len(essential_cols_not_in_df) != 0:
        all_checks_ok = False
        print(
            "Error checking essential columns: Not all the essential columns are in the imported data. Missed: "
            + str(essential_cols_not_in_df)
        )

    if len(check_data_types_result) != 0:
        all_checks_ok = False
        print("Error checking data types: Some data types loaded do not match: " + str(check_data_types_result))

    if check_dataset_size == False:
        all_checks_ok = False
        print("Error checking dataset size: The size of the loaded data does not match.")

    if len(check_null_vars_result) != 0:
        all_checks_ok = False
        print("Error checking nulls: The following columns have null values: " + str(check_null_vars_result))

    if len(check_variable_formats) != 0:
        all_checks_ok = False
        print("Error checking formats: The following columns formats do not match: " + str(check_variable_formats))

    if check_duplicates == False:
        all_checks_ok = False
        print("Error checking duplicates: There are duplicates in the data loaded.")

    if len(check_data_consistency) != 0:
        all_checks_ok = False
        print(
            "Error checking data consistency: The following columns are not consistent: " + str(check_data_consistency)
        )

    if try_catch_failed:
        all_checks_ok = False
        print("Execution error! " + try_catch_failed)

    if all_checks_ok:
        print("All checks passed")
    else:
        print("Check with errors")
        os._exit(os.EX_DATAERR)

    return all_checks_ok
