### the main entry point, and the starter of the application
import psycopg2
from pathlib import Path

def main():
    """
    The main function that starts everything 
    Connects to the DB, reads the config, starts the executor cores, and starts the user interface. 
    """
    try:
        conn = psycopg2.connect(
                host="127.0.0.1",
                port=5432,
                dbname="postgres",
                user="u0_a453"
                )
        conn.autocommit = True
    except Exception as e:
        raise ConnectionError(f"Connection to the posgres database failed. Reason: {e}") from e
    
    curr = conn.cursor()

    main_file = Path(__file__)

    sql_dir = main_file.parent.parent / "sql"

    for i in sorted(sql_dir.glob("*.sql")):
        try:
            curr.execute(i.read_text())
        except Exception as e:
            raise psycopg2.DatabaseError(f"the setup of the db via the sql files failed. reason: {e}") from e
    
    
    

    
if __name__ == "__main__":
    main()
