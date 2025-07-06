import sqlite3

def check_database():
    """Check if database and table exist"""
    try:
        conn = sqlite3.connect('DBLP.db')
        cursor = conn.cursor()
        
        # Check if conf_papers table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='conf_papers'")
        table_exists = cursor.fetchone()
        
        if table_exists:
            print(" conf_papers table exists")
            
            # Check table structure
            cursor.execute("PRAGMA table_info(conf_papers)")
            columns = cursor.fetchall()
            print(f"Table has {len(columns)} columns:")
            for col in columns:
                print(f"  - {col[1]} ({col[2]})")
            
            # Check if there are any records
            cursor.execute("SELECT COUNT(*) FROM conf_papers")
            count = cursor.fetchone()[0]
            print(f"Current records in table: {count}")
            
        else:
            print(" conf_papers table does NOT exist!")
            print("Available tables:")
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            for table in tables:
                print(f"  - {table[0]}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error checking database: {e}")

if __name__ == "__main__":
    check_database()