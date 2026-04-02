### the main entry point, and the starter of the application
import psycopg2
from pathlib import Path

def main():
    """
    The main function that starts everything 
    Connects to the DB, reads the config, starts the executor cores, and starts the user interface. 
    """
    conn = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            dbname="postgres",
            user="u0_a453"
            )
    conn.autocommit = True

    conn.autocommit = True
    curr = conn.cursor()

    main_file = Path(__file__)

    sql_dir = main_file.parent.parent / "sql"

    for i in sorted(sql_dir.glob("*.sql")):
        print(f"executing file {i.name} ...")
        curr.execute(i.read_text())
    
if __name__ == "__main__":
    main()
