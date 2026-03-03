import re
import logging

def execute_sql_file(file_path: str, stop_on_error: bool = True):
    """
    Reads a SQL file and executes statements sequentially in Databricks.

    Parameters:
        file_path (str): DBFS or local path to .sql file
        stop_on_error (bool): If True, stops execution on first error
    """

    # Configure logger
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("SQLExecutor")

    # Read SQL file
    try:
        with open(file_path, "r") as f:
            sql_script = f.read()
    except Exception as e:
        raise RuntimeError(f"Failed to read file: {e}")

    # Remove multiline comments /* */
    sql_script = re.sub(r"/\*.*?\*/", "", sql_script, flags=re.DOTALL)

    # Remove single-line comments --
    sql_script = re.sub(r"--.*", "", sql_script)

    # Split by semicolon safely
    statements = [
        stmt.strip()
        for stmt in sql_script.split(";")
        if stmt.strip()
    ]

    logger.info(f"Found {len(statements)} SQL statements.")

    for i, statement in enumerate(statements, start=1):
        try:
            logger.info(f"Executing statement {i}/{len(statements)}")
            spark.sql(statement)
            logger.info(f"Statement {i} executed successfully.")
        except Exception as e:
            logger.error(f"Error in statement {i}: {e}")
            if stop_on_error:
                raise
            else:
                continue

    logger.info("SQL file execution completed.")