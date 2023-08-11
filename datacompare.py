import psycopg2
from datetime import datetime, timedelta


conn = psycopg2.connect(
    database="DATABASE",
    user="USER",
    password="ADMİN",
    host="LOCALHOST",
    port="1234"
)

today = datetime.now()
days_ago = today - timedelta(days=10)  

cur = conn.cursor()

db1_to_db3_query = f"""
    INSERT INTO db3 (record_type, source, module, count_a, as_of_date)
    SELECT
        db1.record_type,
        db1.source,
        db1.module,
        db1.count_a,
        db1.as_of_date
    FROM db1
    LEFT JOIN db2
        ON db1.source = db2.source
        AND db1.module = db2.module
        AND db1.count_a = db2.count_a
    WHERE db2.source IS NULL;
"""

cur.execute(db1_to_db3_query)
conn.commit()

db2_to_db3_query = f"""
    INSERT INTO db3 (record_type, source, module, count_a, as_of_date)
    SELECT
        db2.record_type,
        db2.source,
        db2.module,
        db2.count_a,
        db2.as_of_date
    FROM db2
    LEFT JOIN db1
        ON db2.source = db1.source
        AND db2.module = db1.module
        AND db2.count_a = db1.count_a
    WHERE db1.source IS NULL;
"""

cur.execute(db2_to_db3_query)
conn.commit()

db1_to_db4_query = f"""
    INSERT INTO db4 (record_type, source, module, count_a, as_of_date)
    SELECT
        record_type,
        source,
        module,
        count_a,
        as_of_date
    FROM db1;

"""

cur.execute(db1_to_db4_query)
conn.commit()

db2_to_db4_query = f"""
    INSERT INTO db4 (record_type, source, module, count_a, as_of_date)
    SELECT
        record_type,
        source,
        module,
        count_a,
        as_of_date
    FROM db2;
"""

cur.execute(db2_to_db4_query)
conn.commit()

db4_db3_to_db5_query = f"""
    INSERT INTO db5 (record_type, source, module, count_a, as_of_date)
    SELECT
        db4.record_type,
        db4.source,
        db4.module,
        db4.count_a,
        db4.as_of_date
    FROM db3
    LEFT JOIN db4
        ON db4.source = db3.source
        AND db4.module = db3.module
        AND db4.count_a = db3.count_a
    WHERE db3.source IS NULL;
"""

cur.execute(db4_db3_to_db5_query)
conn.commit()

cur.close()
conn.close()