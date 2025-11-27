"""Command-line entrypoint for reconciliation utilities."""
from services import DatabaseService


def main() -> None:
    print("Jai Guruji")
    db_service = DatabaseService()

    test_results = db_service.test_connections()
    print("Connection test results:", test_results)

    mysql_count, postgres_count = db_service.counts()
    print("MySQL count:", mysql_count, "Postgres count:", postgres_count)

    db_service.close()


if __name__ == "__main__":
    main()
