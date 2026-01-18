import pandas as pd
from sqlalchemy import create_engine, engine
from sqlalchemy.dialects.postgresql import DATE, INTEGER, TEXT
from sqlalchemy.exc import (
    IntegrityError,
    InternalError,
    NoSuchModuleError,
    OperationalError,
    ProgrammingError,
)


def get_db_engine() -> engine.Engine | None:
    """Create a db engine to be used by other functions

    Args:
        schema: Optional database schema name

    Returns:
        SQLAlchemy engine or None if connection fails
    """

    # get all the environmentt variable
    schema_name = "public"
    host = "pg-yelp"
    port = 5432
    user = "postgres"
    pw = "postgres"
    db = "yelp"
    driver = "postgresql+psycopg2"

    if all([host, port, user, pw, db, driver]):
        db_url = f"{driver}://{user}:{pw}@{host}:{port}/{db}"
        db_url += f"?options=-csearch_path%3D{schema_name}"

        try:
            return create_engine(db_url)
        except ConnectionError as e:
            raise ConnectionError(f"Unable to access database: {e}") from e
        except NoSuchModuleError as e:
            raise NoSuchModuleError(f"Invalid driver specified: {e}") from e

    return None


def read_raw_review_data() -> pd.DataFrame:
    """Generator that reads JSON file in chunks to avoid memory issues"""
    chunk_size = 10000
    for chunk in pd.read_json(
        "data/yelp_academic_dataset_review.json", lines=True, chunksize=chunk_size
    ):
        yield chunk


def load_raw_review_data(engine: engine.Engine) -> None:
    try:
        first_chunk = True
        with engine.connect() as connection:
            for chunk in read_raw_review_data():
                chunk.to_sql(
                    "raw_yelp_review",
                    con=connection,
                    if_exists="replace" if first_chunk else "append",
                    index=False,
                    chunksize=1000,
                    dtype={
                        "review_id": TEXT,
                        "user_id": TEXT,
                        "business_id": TEXT,
                        "stars": INTEGER,
                        "date": DATE,
                        "text": TEXT,
                        "useful": INTEGER,
                        "funny": INTEGER,
                        "cool": INTEGER,
                    },
                )
                first_chunk = False
                print(f"Loaded chunk successfully.")
    except (IntegrityError, InternalError, OperationalError, ProgrammingError) as e:
        raise RuntimeError(f"Error loading data into database: {e}") from e


if __name__ == "__main__":
    engine = get_db_engine()
    if engine is not None:
        load_raw_review_data(engine)
        print("Raw review data loaded successfully.")
    else:
        print("Failed to create database engine.")
