# scripts

Companion scripts that build on bintrail. These are examples, not part of the
`bintrail` binary.

## adaptive_throttle.py

Adaptive write throttle for MySQL through ProxySQL, driven by binlog change
volume. The trigger is replica apply lag (`Seconds_Behind_Source`); when a
replica falls behind, the script asks the bintrail index which table is
producing the most binlog change right now, and installs a per-rule `delay` in
ProxySQL that throttles writes to that one table. It clears the throttle when
the replicas catch up.

It watches one or more replicas of the same primary. Because every replica
replays the same primary's binlog, the hot table and the ProxySQL target are
shared, so only the lag trigger is per-replica: it engages when the **worst**
replica crosses the lag threshold and releases when **every** replica is back
under it. A replica that is unreachable is logged and skipped, never aborting
the others, and it reconnects on its own when it comes back. (This assumes one
primary; replicas of different primaries would need a throttle per primary.)

It is pure open-source: it talks only to your own MySQL, your ProxySQL admin
interface, and the bintrail index database. No hosted service, no API key, no
external calls.

Why writes only: replica apply lag is a function of binlog volume, and only
writes go into the binlog. The index records binlog row events, so ranking by
`COUNT(*)` per table ranks write hotspots by definition, never reads.

The hot-table lookup is a plain aggregation over the `binlog_events` table that
`bintrail stream` maintains:

```sql
SELECT schema_name, table_name, COUNT(*) AS c
FROM binlog_events
WHERE event_timestamp >= UTC_TIMESTAMP() - INTERVAL 60 SECOND
GROUP BY schema_name, table_name
ORDER BY c DESC
LIMIT 1;
```

Dependencies and assumptions:

- `pip install pymysql`
- A running `bintrail stream` writing the `binlog_events` index (see the main
  bintrail docs). Point `INDEX` in the config block at that database.
- A ProxySQL admin interface (default port 6032). On ProxySQL 3.0 the default
  `admin` user only connects locally, so run this on the ProxySQL host or add a
  non-local admin credential.
- One or more read replicas reachable for `SHOW REPLICA STATUS` (MySQL
  8.0.22+ / 8.4), listed in `REPLICAS`.

Edit the config block at the top (`PROXYSQL`, `REPLICAS`, `INDEX`, thresholds)
before running. See the companion blog post for the full write-up.
