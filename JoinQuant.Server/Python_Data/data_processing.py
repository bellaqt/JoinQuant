import boto3
import json
import requests
from sqlalchemy import create_engine
from botocore.exceptions import ClientError
from datetime import date

today = date.today().strftime("%Y-%m-%d")

REGION = "ap-southeast-2"
FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
RDS_SECRET_NAME = "rds!db-fab82ed6-ca28-4884-82a3-1b311494c065"

# AWS clients
ssm = boto3.client("ssm", region_name=REGION)
secrets_client = boto3.client("secretsmanager", region_name=REGION)
# AWS Email Service client
ses = boto3.client("ses", region_name=REGION)

# SQL to create series table
CREATE_SERIES_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS fred_series (
      series_id VARCHAR(64) PRIMARY KEY,
      title_cn VARCHAR(255) NOT NULL,
      link VARCHAR(512) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

UPSERT_SERIES_SQL = """
INSERT INTO fred_series (series_id, title_cn, link)
VALUES (%(series_id)s, %(title_cn)s, %(link)s)
ON DUPLICATE KEY UPDATE
  title_cn = IF(VALUES(title_cn) <> title_cn, VALUES(title_cn), title_cn),
  link     = IF(VALUES(link)     <> link,     VALUES(link),     link)
"""

#SQL to create observations table
CREATE_OBSERVATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS observations (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    series_id VARCHAR(64) NOT NULL,
    frequency VARCHAR(16) NOT NULL,
    `limit` INT,
    channel_name VARCHAR(16) NOT NULL,
    obs_date DATE NOT NULL,
    value DECIMAL(20,6),
    value_unit VARCHAR(16) NOT NULL,

    UNIQUE KEY uk_series_channel_date (series_id, channel_name, obs_date),
    FOREIGN KEY (series_id) REFERENCES fred_series(series_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# ---------- config ----------
def get_fred_api_key():
    return ssm.get_parameter(
        Name="/joinquant/dev/fred/apikey",
        WithDecryption=True
    )["Parameter"]["Value"]


def load_series_config():
    series1_raw = ssm.get_parameter(
        Name="/joinquant/dev/fred/series1",
        WithDecryption=True
    )["Parameter"]["Value"]

    series2_raw = ssm.get_parameter(
        Name="/joinquant/dev/fred/series2",
        WithDecryption=True
    )["Parameter"]["Value"]

    series1 = json.loads(series1_raw)["series"]
    series2 = json.loads(series2_raw)["series"]

    return {**series1, **series2}


# ---------- secrets ----------
def get_rds_secret():
    try:
        resp = secrets_client.get_secret_value(SecretId=RDS_SECRET_NAME)
        return json.loads(resp["SecretString"])
    except ClientError as e:
        raise RuntimeError("Failed to load RDS secret") from e


# ---------- fred ----------
def fetch_series(series_id, channel_cfg, api_key):
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "frequency": channel_cfg["frequency"],
        "limit": channel_cfg["limit"]
    }
    r = requests.get(FRED_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()["observations"]

def parse_value(v):
    return None if v == "." else float(v)


def get_mysql_engine():
    cred = get_rds_secret()

    db_host = "joinquant.claegyoiyrwn.ap-southeast-2.rds.amazonaws.com"
    db_port = 3306
    db_name = "joinquant"

    engine = create_engine(
        f"mysql+pymysql://{cred['username']}:{cred['password']}"
        f"@{db_host}:{db_port}/",
        pool_recycle=3600
    )
    
    """engine = create_engine(
        "mysql+pymysql://admin:mhvV5*pu_9)Lj9.qPM7ScK9z0mrv@127.0.0.1:3307/joinquant"
    )"""

    with engine.begin() as conn:
        conn.exec_driver_sql(
            f"""
            CREATE DATABASE IF NOT EXISTS {db_name}
            CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci
            """
        )
        conn.exec_driver_sql(f"USE {db_name}")
    return engine

def create_series_table(conn):
    conn.exec_driver_sql(CREATE_SERIES_TABLE_SQL)
    conn.exec_driver_sql(CREATE_OBSERVATIONS_TABLE_SQL)

def upsert_series(conn, series_id, cfg):
    print(f"Upserting series: {series_id} - {cfg['title_cn']}")
    conn.exec_driver_sql(
        UPSERT_SERIES_SQL,
        {
            "series_id": series_id,
            "title_cn": cfg["title_cn"],
            "link": cfg["link"],
        }
    )

def delete_missing_series(conn, valid_series_ids):
    if not valid_series_ids:
        return 

    placeholders = ",".join(["%s"] * len(valid_series_ids))
    sql = f"""
    DELETE FROM fred_series
    WHERE series_id NOT IN ({placeholders})
    """

    conn.exec_driver_sql(sql, tuple(valid_series_ids))

def convert_value(raw_value, value_unit):
    if raw_value is None:
        return None

    if value_unit == 'Millions':
        return raw_value * 0.01     
    elif value_unit == 'Billions':
        return raw_value * 10       
    else:
        return raw_value    
    
def converted_value_u(value_unit):
    return '\u4ebf(100 million)'   

def get_series_unit(conn, series_id, cache={}):
    if series_id in cache:
        return cache[series_id]

    res = conn.exec_driver_sql(
        "SELECT value_unit FROM fred_series WHERE series_id = %s",
        (series_id,)
    ).fetchone()

    unit = res[0] if res else 'number'
    cache[series_id] = unit
    return unit

def insert_series_values(
    conn,
    series_id,
    channel_name,
    channel_cfg,
    observations
):
    value_unit = get_series_unit(conn, series_id)
    sql = """
        INSERT INTO observations
        (series_id, frequency, `limit`, channel_name, obs_date, value, value_unit)
        VALUES
        (%(series_id)s, %(frequency)s, %(limit)s, %(channel_name)s, %(obs_date)s, %(value)s, %(value_unit)s)
        ON DUPLICATE KEY UPDATE
            value     = VALUES(value),
            frequency = VALUES(frequency),
            `limit`   = VALUES(`limit`),
            value_unit = VALUES(value_unit)
    """

    rows = []
    for obs in observations:
        raw_value = parse_value(obs["value"])
        converted_value = convert_value(raw_value, value_unit)
        converted_value_unit = converted_value_u(value_unit)

        rows.append({
            "series_id": series_id,
            "frequency": channel_cfg["frequency"],
            "limit": channel_cfg.get("limit"),
            "channel_name": channel_name,
            "obs_date": obs["date"],  
            "value": converted_value,
            "value_unit": converted_value_unit
        })

    if not rows:
        return

    conn.exec_driver_sql(sql, rows)

def cleanup_observations(conn):
    sql = """
    DELETE o
    FROM observations o
    JOIN (
        SELECT id
        FROM (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY series_id, channel_name
                    ORDER BY obs_date DESC
                ) AS rn,
                `limit`
            FROM observations
        ) t
        WHERE rn > `limit`
    ) d ON o.id = d.id;
    """
    conn.exec_driver_sql(sql)

def query_mail_observations(conn):
    sql = """
    SELECT
        o.series_id,
        o.frequency,
        s.title_cn,
        o.obs_date,
        o.value,
        o.value_unit
    FROM observations o
    JOIN fred_series s
      ON o.series_id = s.series_id
    WHERE o.channel_name = 'mail'
    ORDER BY o.series_id, o.obs_date DESC
    """
    return conn.exec_driver_sql(sql).fetchall()

def build_mail_body(rows):
    print("Building email body...")
    if not rows:
        return None

    lines = ["Daily FRED Update (\u6570\u636e\u4e3a\u6700\u65b0\u4e00\u671f)\n"]
    current_series = None

    for r in rows:
        if r.series_id != current_series:
            if current_series is not None:
                lines.append("<br>")

            web_url = f"http://JoinquantTarget-1804201283.ap-southeast-2.elb.amazonaws.com/web/series/{r.series_id}"

            lines.append(
                f'<p><strong>'
                f'<a href="{web_url}">{r.title_cn}</a>'
                f'</strong></p>'
            )

            current_series = r.series_id

        lines.append(
            f"<li>{r.frequency} | {r.obs_date} | "
            f"{r.value} {r.value_unit}</li>"
        )

    print("Email body built.")
    return "\n".join(lines)

def send_mail(body):
    print("Sending email...")
    resp = ses.send_email(
        Source="dou20254@gmail.com",
        Destination={"ToAddresses": ["chang20204@gmail.com","syh227ss@163.com"]},
        Message={
            "Subject": {
                "Data": f"Daily FRED Update ({today})",
                "Charset": "UTF-8"
            },
            "Body": {
                "Html": {
                    "Data": body,
                    "Charset": "UTF-8"
                }
            }
        }
    )
    print("SES response:", resp)

# ---------- pipeline ----------
def run_fred_pipeline():
    api_key = get_fred_api_key()
    series_config = load_series_config()
    engine = get_mysql_engine()
    
    with engine.begin() as conn:
        create_series_table(conn)
        for series_id, cfg in series_config.items():
               #print("\nseries_id:", series_id)
               #print("  CN:", cfg["title_cn"])
               upsert_series(conn, series_id, cfg)

               for channel_name, channel_cfg in cfg["channels"].items():
                    try:
                        observations = fetch_series(series_id, channel_cfg, api_key)
                    except Exception as e:
                        print(f"fetch failed for {series_id}: {e}")
                        continue

                    insert_series_values(
                        conn,
                        series_id,
                        channel_name,
                        channel_cfg,
                        observations
                    )
               cleanup_observations(conn)

        delete_missing_series(conn, list(series_config.keys()))
        rows = query_mail_observations(conn)
        body = build_mail_body(rows)

        if body:
            send_mail(body)
    print("FRED pipeline completed.")

# ---------- main ----------
def main():
    run_fred_pipeline()


if __name__ == "__main__":
    main()
