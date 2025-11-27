"""Legacy classifier proof-of-concept pipeline."""
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

DATABASE_URL = "mysql+pymysql://user:pass@Kartik-iMac24.local:3306/legacy_security_master"
engine = create_engine(os.getenv("DATABASE_URL"))
db = scoped_session(sessionmaker(bind=engine))


def main():
    records = db.execute("select * from legacy_security_master.security_master").fetchall()
    for record in records:
        print(f"{record.instrument_id}, {record.as_of_date}")


if __name__ == "__main__":
    main()
