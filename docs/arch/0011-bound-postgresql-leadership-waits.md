# 11. Bound PostgreSQL leadership waits

Date: 2026-08-04

## Status

Accepted

Amends [4. Coordinate multi-host scaling in PostgreSQL](0004-coordinate-multi-host-scaling-in-postgresql.md)

## Context

ADR 4 chose a dedicated blocking PostgreSQL advisory-lock connection for each
standby. This lets PostgreSQL hand leadership directly to a waiter without an
application polling interval. The daemon also registers one contender per node
so repeated heartbeats cannot open duplicate waiting connections.

A transport failure can leave a client-side lock wait blocked even after the
leader's bounded lease check has failed and that node has returned to standby.
The registered contender then prevents the node from opening another wait. If
every contender is stranded in that state, PostgreSQL may be reachable again
while the cluster has no process able to acquire leadership.

Session advisory locks remain tied to their PostgreSQL sessions. Canceling a
pgx operation closes its underlying connection by default, which provides a
safe way to abandon an ambiguous wait without creating a second active lease
for the same node.

## Decision

We will limit each blocking leadership wait to 30 seconds. An expired wait will
be canceled with its dedicated connection and immediately replaced with a fresh
blocking wait. Routine expiry will not be logged as an election failure and will
not use failure backoff.

Connection setup failures, PostgreSQL errors, and other non-timeout failures
will retain their error diagnostics and two-second retry delay. The wait bound
will remain an internal reliability policy rather than operator configuration.

## Consequences

A standby can recover from a stranded connection without process intervention.
After PostgreSQL connectivity returns, a fresh contender begins within roughly
30 seconds even if the previous network operation never completed normally.
Healthy leadership handoff remains driven by PostgreSQL's waiter queue rather
than periodic try-lock polling.

Each standby replaces its dedicated waiting connection approximately twice per
minute while another leader remains healthy. This is bounded, predictable
connection turnover in exchange for bounded recovery. Canceling an ambiguous
wait may briefly leave driver cleanup in progress, but the canceled connection
cannot retain a durable leadership term after its PostgreSQL session ends.
