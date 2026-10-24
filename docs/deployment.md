# Deploy RunnerFoundry

Choose SQLite for one RunnerFoundry member. Choose PostgreSQL when you want PostgreSQL-backed state or several members.
The database changes where RunnerFoundry stores durable state. It does not change the [job lifecycle](PRD.md).

Complete the [installation and GitHub App setup](installation.md) on each host first. The examples below use the packaged systemd service and protected credential files.

Prepare a runner provider before starting RunnerFoundry. For the Docker examples, create and validate `/etc/runnerfoundry/compose.yaml` through [configure a runner and its pool](../README.md#2-configure-a-runner-and-its-pool). For a different Docker service or for libvirt, follow the matching [provider contract](providers.md).

The standalone YAML examples below completely replace `/etc/runnerfoundry/config.yaml`. Substitute your numeric GitHub App ID for `123456` and confirm every referenced provider and credential file exists. Cluster snippets show the sections which must agree or differ between otherwise complete member configurations.

## Standalone deployment

A standalone deployment has one RunnerFoundry member. SQLite is the simplest option when all capacity is on one host.

### Use SQLite on one host

Configure one member with a local database:

```yaml
github:
  apps:
    - app_id: 123456
      private_key_file: /etc/runnerfoundry/github-app.pem

state:
  path: /var/lib/runnerfoundry/state.db

scale_sets:
  - name: linux-docker
    max_runners: 4
    docker:
      compose_file: /etc/runnerfoundry/compose.yaml
      service: runner
```

Only one RunnerFoundry member may use this database. Do not put the SQLite database on shared storage or open it from another member.

Start the member and confirm it becomes ready:

```sh
sudo systemctl enable --now runnerfoundry
sudo systemctl status runnerfoundry
```

Use the [SQLite backup procedure](operations.md#back-up-and-upgrade-sqlite) before upgrading or moving the database.

### Use PostgreSQL on one host

RunnerFoundry supports PostgreSQL 17 and 18. A single member using PostgreSQL is still a standalone deployment.

In a PostgreSQL administration client, use an identity with authority to create roles and databases. Create one stable `NOLOGIN` role for RunnerFoundry-owned objects and a login identity which can assume it:

```sql
CREATE ROLE runnerfoundry
  NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;

CREATE ROLE runnerfoundry_login
  LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;

GRANT runnerfoundry TO runnerfoundry_login;

CREATE DATABASE runnerfoundry OWNER runnerfoundry;
```

Set authentication for `runnerfoundry_login` through your PostgreSQL administration process. Keep the stable role separate from login credentials so you can rotate or replace login identities without changing database ownership.

Create files which the packaged service can read:

```sh
sudo install -d -m 0750 -o root -g runnerfoundry /etc/runnerfoundry/secrets
printf '%s' 'runnerfoundry_login' | sudo tee /etc/runnerfoundry/secrets/postgres-user >/dev/null
read -r -s -p 'PostgreSQL password: ' RUNNERFOUNDRY_PASSWORD; printf '\n'
printf '%s' "$RUNNERFOUNDRY_PASSWORD" | sudo tee /etc/runnerfoundry/secrets/postgres-password >/dev/null
unset RUNNERFOUNDRY_PASSWORD
sudo chown root:runnerfoundry /etc/runnerfoundry/secrets/postgres-user /etc/runnerfoundry/secrets/postgres-password
sudo chmod 0640 /etc/runnerfoundry/secrets/postgres-user /etc/runnerfoundry/secrets/postgres-password
sudo -u runnerfoundry test -r /etc/runnerfoundry/secrets/postgres-user
sudo -u runnerfoundry test -r /etc/runnerfoundry/secrets/postgres-password
```

The prompt keeps the password out of shell history. RunnerFoundry removes surrounding whitespace when it reads either file.

Use PostgreSQL instead of `state.path`:

```yaml
github:
  apps:
    - app_id: 123456
      private_key_file: /etc/runnerfoundry/github-app.pem

state:
  postgres:
    role: runnerfoundry
    host: postgresql.example.net
    port: "5432"
    name: runnerfoundry
    user: file:///etc/runnerfoundry/secrets/postgres-user
    password: file:///etc/runnerfoundry/secrets/postgres-password

scale_sets:
  - name: linux-docker
    max_runners: 4
    docker:
      compose_file: /etc/runnerfoundry/compose.yaml
      service: runner
```

Start the service. RunnerFoundry applies known forward PostgreSQL migrations during startup:

```sh
sudo systemctl enable --now runnerfoundry
sudo systemctl status runnerfoundry
```

Use the [controlled PostgreSQL upgrade procedure](operations.md#upgrade-postgresql) for later upgrades.

## Clustered deployment

A clustered deployment has 2 or more members sharing one PostgreSQL database. Members coordinate durable work and GitHub-facing leadership through that database. You do not designate a leader.

### Prepare the shared database

Use PostgreSQL 17 or 18. Create the database, stable role and login as described in [standalone PostgreSQL setup](#use-postgresql-on-one-host). You may give each host a separate login identity. Every login must be able to assume the same stable role.

Install protected credential files on every member. Each member must point to the same database:

```yaml
state:
  postgres:
    role: runnerfoundry
    host: postgresql.example.net
    port: "5432"
    name: runnerfoundry
    user: file:///etc/runnerfoundry/secrets/postgres-user
    password: file:///etc/runnerfoundry/secrets/postgres-password
```

If the primary endpoint does not preserve PostgreSQL sessions, provide a session-affine direct endpoint:

```yaml
state:
  postgres:
    role: runnerfoundry
    host: postgresql-pool.example.net
    port: "5432"
    name: runnerfoundry
    user: file:///etc/runnerfoundry/secrets/postgres-user
    password: file:///etc/runnerfoundry/secrets/postgres-password
    direct:
      host: postgresql-direct.example.net
```

The omitted direct fields inherit their primary structured values. See the [direct PostgreSQL connection reference](configuration.md#direct-postgresql-connection) before using DSNs or separate direct credentials.

### Give every member a stable identity

RunnerFoundry uses the operating system hostname as its member identity. Give every member a stable, unique hostname before starting it. Never run 2 members with the same hostname against one shared database.

### Keep GitHub Apps consistent

Use the same ordered `github.apps` list on every member. Order decides repository precedence when several Apps can see the same repository. Install each private key at the configured path on every member.

Different App order can give members conflicting views of route ownership. Check the final files before starting the cluster:

```sh
sudo -u runnerfoundry test -r /etc/runnerfoundry/config.yaml
sudo -u runnerfoundry test -r /etc/runnerfoundry/github-app.pem
```

### Distribute Runner Pool capacity

Members may declare different Runner Pools. They may also contribute capacity to the same pool. Each live member's `max_runners` adds to that pool's shared capacity.

For example, if 2 live members each configure this pool, the cluster contributes 8 runners of configured capacity:

```yaml
scale_sets:
  - name: linux
    runner_group: default
    max_runners: 4
    docker:
      compose_file: /etc/runnerfoundry/compose.yaml
      service: runner
```

Every member declaring the same Runner Pool must use the same `runner_group`. Members may use different local provider kinds for that pool, but a job can then run in either environment. Use mixed providers only when their images, tools and security boundaries are intentionally interchangeable.

### Start and confirm the cluster

Start RunnerFoundry on each host:

```sh
sudo systemctl enable --now runnerfoundry
sudo systemctl status runnerfoundry
```

One member reports `leadership=leader`. Healthy followers report `leadership=standby`, which is normal. Confirm that each member is operational and that shared pools appear in GitHub before sending production work. Use [cluster health and logs](operations.md#check-health-and-leadership) when a member is degraded.

