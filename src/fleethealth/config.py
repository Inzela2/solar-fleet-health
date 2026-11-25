import os

DSN = os.environ.get("FLEET_DSN", "postgresql://solar:solar@localhost:5432/solar")
CLICKHOUSE_DSN = os.environ.get("FLEET_CH_DSN", "clickhouse://default:@localhost:8123/default")
TZ = "Australia/Sydney"
