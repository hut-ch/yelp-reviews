import pandas as pd
from sqlalchemy import create_engine, engine
from sqlalchemy.dialects.postgresql import BOOLEAN, FLOAT, INTEGER, JSONB, TEXT
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


def read_raw_business_data() -> pd.DataFrame:
    df = pd.read_json("data/yelp_academic_dataset_business.json", lines=True)

    return df


def load_raw_business_data(df: pd.DataFrame, engine: engine.Engine) -> None:
    try:
        with engine.connect() as connection:
            df.to_sql(
                "raw_yelp_business",
                con=connection,
                if_exists="replace",
                index=False,
                chunksize=1000,
                dtype={
                    "business_id": TEXT,
                    "name": TEXT,
                    "address": TEXT,
                    "city": TEXT,
                    "state": TEXT,
                    "postal_code": TEXT,
                    "latitude": FLOAT,
                    "longitude": FLOAT,
                    "stars": FLOAT,
                    "review_count": INTEGER,
                    "is_open": BOOLEAN,
                    "attributes": JSONB,
                    "categories": TEXT,
                    "hours": JSONB,
                },
            )
            print("Data loaded successfully.")
    except (IntegrityError, InternalError, OperationalError, ProgrammingError) as e:
        raise RuntimeError(f"Error loading data into database: {e}") from e


if __name__ == "__main__":
    engine = get_db_engine()
    if engine is not None:
        business_df = read_raw_business_data()
        load_raw_business_data(business_df, engine)
        print("Raw business data loaded successfully.")
    else:
        print("Failed to create database engine.")
