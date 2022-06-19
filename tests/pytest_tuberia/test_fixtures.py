def test_test_database(spark, test_database):
    # Following line fails if the database do not exist.
    spark.catalog.listTables(test_database)
