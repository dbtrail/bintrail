# scripts

Companion scripts that build on bintrail / dbtrail. These are examples, not part
of the `bintrail` binary.

## adaptive_throttle.py

Adaptive write throttle for MySQL through ProxySQL, driven by binlog change
volume. The trigger is replica apply lag (`Seconds_Behind_Source`); when the
replica falls behind, the script asks dbtrail's `count_events` MCP tool which
table is producing the most binlog change right now, and installs a per-rule
`delay` in ProxySQL that throttles writes to that one table. It clears the
throttle when the replica catches up.

Why writes only: replica apply lag is a function of binlog volume, and only
writes go into the binlog. `count_events` reads the binlog, so it ranks write
hotspots by definition, never reads.

Dependencies and assumptions:

- `pip install pymysql requests`
- A ProxySQL admin interface (default port 6032). On ProxySQL 3.0 the default
  `admin` user only connects locally, so run this on the ProxySQL host or add a
  non-local admin credential.
- A read replica reachable for `SHOW REPLICA STATUS` (MySQL 8.0.22+ / 8.4).
- A dbtrail account with an API key (`bt_...`) and a `server_id`. The
  `count_events` call goes to the dbtrail hosted MCP gateway, so this script
  needs that SaaS, it is not pure open-source bintrail.

Edit the config block at the top (`PROXYSQL`, `REPLICA`, `DBTRAIL_*`,
thresholds) before running. See the companion blog post for the full write-up.
