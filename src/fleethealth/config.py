import os

DSN = os.environ.get("FLEET_DSN", "postgresql://solar:solar@localhost:5432/solar")
TZ = "Australia/Sydney"
