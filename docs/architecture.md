# Architecture

`runnerfoundry` is a long-running Go application that turns GitHub Actions runner scale-set demand into ephemeral Docker containers or libvirt virtual machines. It discovers repositories through a GitHub App, creates and maintains runner scale sets for those repositories, receives GitHub's scale-set messages, admits work within configured capacity, requests just-in-time (JIT) runner configuration, and starts one provider instance for each admitted runner.

The same executable supports two runtime shapes:

- Standalone mode coordinates one provider host and keeps its durable FIFO demand queue in an exclusively owned SQLite database.
- Clustered mode combines capacity from multiple provider hosts. PostgreSQL holds shared declarations, health, demand, assignments, and node-owned Docker or libvirt lifecycle obligations, while a session advisory lock selects the one host that manages GitHub-facing scale-set sessions.

Both modes preserve identifiable job demand before acknowledging the GitHub message that delivered it. Neither mode can make GitHub, the state database, and the provider participate in one transaction. Stable request-derived runner names and explicit persisted states make partial operations recoverable without claiming exactly-once execution.

## Capabilities

The application currently provides the following behavior:

- GitHub App authentication without storing the private key in either state database.
- Discovery of every installation owned by the App and every repository visible to each installation.
- Creation and deletion of repository runner scale sets and their message sessions.
- One FIFO per scale-set name, ordered first by GitHub queue time and then by local observation order.
- A shared capacity limit across repositories in standalone mode.
- Per-host capacity contributions, global scheduling, and leader failover in clustered mode.
- Ephemeral runner provisioning through Docker Compose or libvirt.
- Durable recovery of positive-identity demand across restart and reload.
- Durable provider-independent runner ownership with a bounded waiting lease.
- Optional removal of unmanaged GitHub runner scale sets during standalone repository discovery.
- Validated `SIGHUP` reload with mode-specific replacement behavior.
- Automatic upward PostgreSQL migration at clustered startup and explicit status, upgrade, and rollback commands.
- Text or JSON structured logging to standard output or one exclusively selected rotating file, plus systemd readiness, reload, and clustered-status notifications. File logging ensures its parent directory exists, while the packaged systemd unit provisions the standard `/var/log/runnerfoundry` path. Logging reloads prepare a replacement before atomically swapping the process-wide handler; exceptional bootstrap, destination, maintenance, and terminal failures remain visible on standard error.

## System context

The operator supplies configuration and credentials and observes the service. GitHub is the authority for installation access, repositories, runner groups, scale sets, job demand, acquisition, and runner registration. Docker Engine or libvirt owns the actual runner process environment. `runnerfoundry` coordinates these systems but does not execute workflow steps itself.

```mermaid
C4Context
    title System Context - runnerfoundry

    Person(operator, "Operator", "Configures, runs, reloads, migrates, and observes the scaler")
    System(scaler, "runnerfoundry", "Discovers repositories, manages runner scale sets, schedules demand, and provisions ephemeral runners")
    System_Ext(github, "GitHub Actions", "Owns App installations, repositories, runner scale sets, job messages, acquisition, and JIT runner registration")
    System_Ext(docker, "Docker Engine", "Runs ephemeral GitHub Actions runner containers")
    System_Ext(libvirt, "libvirt", "Runs ephemeral GitHub Actions runner virtual machines")

    Rel(operator, scaler, "Configures and operates")
    Rel(scaler, github, "Discovers repositories and manages runner scale sets", "HTTPS")
    Rel(github, scaler, "Supplies job demand and lifecycle messages", "Scale-set message sessions")
    Rel(scaler, docker, "Lists, starts, and destroys owned runner containers", "docker compose CLI")
    Rel(scaler, libvirt, "Lists, starts, and destroys owned runner domains", "virsh CLI")
```

## Runtime containers

The executable is the only application process supplied by this project. It runs either the daemon command or a PostgreSQL migration command. The daemon selects exactly one state backend from configuration. SQLite belongs to one standalone process; the PostgreSQL schema is shared by every process in a cluster.

```mermaid
C4Container
    title Container Diagram - runnerfoundry

    Person(operator, "Operator", "Runs the executable and supplies configuration")
    System_Ext(github, "GitHub Actions", "Repository, runner scale-set, message, acquisition, and JIT APIs")
    System_Ext(docker, "Docker Engine", "Ephemeral container execution")
    System_Ext(libvirt, "libvirt", "Ephemeral virtual-machine execution")

    System_Boundary(scaler_system, "runnerfoundry") {
        Container(executable, "runnerfoundry executable", "Go", "Runs the daemon or PostgreSQL migration commands")
        ContainerDb(sqlite, "Standalone state", "SQLite", "Exclusively owned durable FIFO demand queue")
        ContainerDb(postgres, "Cluster state", "PostgreSQL schema", "Shared nodes, contributions, demand, assignments, compatibility metadata, and advisory locks")
    }

    Rel(operator, executable, "Starts, reloads, stops, and invokes migrations")
    Rel(executable, github, "Discovers and manages repositories and runners", "HTTPS")
    Rel(executable, docker, "Controls runner containers", "docker compose CLI")
    Rel(executable, libvirt, "Controls runner domains", "virsh CLI")
    Rel(executable, sqlite, "Reads and transactionally updates in standalone mode", "database/sql")
    Rel(executable, postgres, "Migrates, publishes, elects, schedules, and executes in clustered mode", "pgx")
```

SQLite and PostgreSQL are alternatives, not replicas of one another. Changing the configured backend requires a restart. Standalone state cannot coordinate multiple processes. Cluster state is designed specifically to make every scheduling decision and recovery transition visible to all participating hosts.

## Process composition

### Command and startup path

The executable installs cancellation handlers for `SIGINT` and `SIGTERM` and coalesces `SIGHUP` into reload requests. It then dispatches either a migration subcommand or the daemon.

Daemon startup performs the following sequence:

1. Read the YAML file through a rooted filesystem handle and reject unknown fields.
2. Apply defaults and validate GitHub App, logging, state, scale-set identity, capacity, and provider settings.
3. Read and trim the GitHub App private-key file.
4. Select clustered mode when `state.postgres` exists; otherwise select standalone mode and `state.path`.
5. Compose mode-specific state, discovery, GitHub, scheduling, provider, lifecycle, logging, and notification components.
6. Complete the mode's local preflight before notifying systemd that the process is ready.

Migration commands take a narrower path. They decode and validate only `state.postgres`, so unrelated daemon configuration cannot prevent an operator from inspecting or repairing the database schema.

```mermaid
flowchart TD
    Start[Process start] --> Signals[Install termination and reload signal handling]
    Signals --> Dispatch{Command}
    Dispatch -->|migrate status, up, or down| MigrationConfig[Load only PostgreSQL state configuration]
    MigrationConfig --> MigrationStore[Inspect or migrate embedded PostgreSQL schema]
    Dispatch -->|daemon| FullConfig[Load and validate complete configuration]
    FullConfig --> Credentials[Read GitHub App private key]
    Credentials --> StateMode{Configured state}
    StateMode -->|state.path| Standalone[Compose standalone daemon]
    StateMode -->|state.postgres| Clustered[Compose clustered daemon]
```

### Shared components

The daemon package is the composition root. It connects configuration and credentials to narrow interfaces implemented by discovery, scale-set, scheduler, state, JIT, and provider components.

```mermaid
C4Component
    title Component Diagram - Shared Daemon Composition

    Container_Boundary(process, "runnerfoundry executable") {
        Component(cli, "Command dispatcher", "cmd/runnerfoundry", "Routes daemon and migration commands and owns process signals")
        Component(config, "Configuration loader", "internal/config and internal/postgresconfig", "Strictly decodes, defaults, validates, and resolves configured state values")
        Component(composition, "Daemon composition", "internal/daemon", "Selects a runtime mode and wires production dependencies")
        Component(discovery, "GitHub App discovery", "internal/githubapp", "Lists App installations and installation repositories")
        Component(sessions, "Scale-set sessions", "internal/daemon", "Creates scale sets, opens message sessions, and adapts GitHub clients")
        Component(poller, "Message poller", "internal/scaleset", "Delivers a message to its handler before acknowledging it")
        Component(reconciler, "Runner reconciler", "internal/scaler", "Reconciles provider-visible runners and obtains JIT configuration")
        Component(providers, "Provider adapters", "internal/daemon and internal/provider", "Translate runner operations into Docker Compose or libvirt commands")
        Component(lifecycle, "Lifecycle and observability", "internal/daemon", "Coordinates reload, shutdown, structured logs, and systemd notifications")
    }

    Rel(cli, config, "Requests configuration")
    Rel(cli, composition, "Runs daemon")
    Rel(composition, discovery, "Creates with App credentials")
    Rel(composition, sessions, "Creates repository scale-set sessions")
    Rel(sessions, poller, "Starts one poller per active repository and scale set")
    Rel(poller, reconciler, "Causes admitted runner work through a mode-specific handler")
    Rel(reconciler, providers, "Lists, starts, and destroys runner instances")
    Rel(composition, lifecycle, "Reports and controls process lifecycle")
```

## GitHub control plane

### Repository authority and discovery

The GitHub App installation boundary decides which repositories the scaler may manage. The discovery client first lists App installations and then uses an installation-authenticated client to list repositories. Installation clients are cached by installation ID.

Failure to list installations fails the discovery pass because no reliable repository authority is available. Failure to list one installation's repositories is isolated and returned alongside successful repositories. Standalone reconciliation retains the last-known repositories for a failed installation so a transient installation failure does not look like access revocation. A repository missing from a successful discovery result is outside the current authority and is removed immediately.

For each desired repository, the scaler audits the complete configured registration set before it admits work. One snapshot is read per distinct runner group. Missing scale sets are created independently, so a later failure never rolls back registrations that already succeeded. Message polling and scheduling begin only after every registration and every session in the repository generation succeeds. A partial generation is closed and detached without deleting registrations, demand, runners, or lifecycle ownership.

Each active session records its confirmed GitHub scale-set ID. Discovery-cadence audits compare that ID with the current registration snapshot. A transient audit failure leaves a healthy generation serving work and reports degraded reconciliation. Confirmed absence or ID replacement suspends the repository, repairs registrations, replaces its complete session group, and resumes admission atomically. A cluster leader applies the same contract to the union declared in PostgreSQL, including scale sets that the leader cannot run locally.

### Message acknowledgement boundary

Every poller receives at most one message at a time. A message must include GitHub statistics. The handler validates and persists its effects, performs the applicable scheduling work, and returns success. Only then does the poller delete the GitHub message and advance its last-message ID.

If validation, persistence, acquisition, scheduling, provider inspection, or provider startup fails, the message remains unacknowledged. GitHub may redeliver it. Demand keys and state transitions are therefore idempotent at the database boundary. A terminal `JobCompleted` message without a positive request ID is a permanent exception: clustered mode reconciles lifecycle cleanup by its stable runner name when available, warns, and acknowledges the message without changing assignment or demand state. A failure during that reconciliation remains retryable.

Pollers issue the next receive immediately after a successful empty or handled response. A failed poll waits five seconds before retrying, and the third consecutive failure adds a sustained-degradation log entry. Cancellation interrupts both an active poll and its retry wait.

Positive request identities and queue timestamps come from `JobAvailable` messages. `JobStarted` first persists the exact runner as busy. Terminal `JobCompleted` atomically requests exact runner cleanup and removes terminal demand. In clustered mode, `JobStarted` retains demand and assignments; only `JobCompleted` makes them terminal. Identity-free assigned capacity is represented by GitHub's authoritative `TotalAssignedJobs` statistic in standalone mode rather than by a durable queue row.

### Scale-set ownership and pruning

The scaler uses the configured scale-set name as the GitHub-facing routing identity. It creates the definition in each managed repository and opens one message session for each repository and scale-set pair.

In standalone mode, `prune: true` lists scale sets in the configured runner groups and deletes names absent from local configuration. This is safe only when the scaler is the sole owner of scale sets in those groups and repositories. Repository access removal is stronger than ordinary shutdown: the scaler removes that repository's queued demand, destroys its active local runners, closes its sessions, and attempts to delete its managed scale sets.

Clustered ownership follows declarations in PostgreSQL. Loss of a node does not withdraw its declarations. An explicit publication that omits a previous scale set withdraws it. The leader deletes a GitHub scale set only when no node declares that name.

## Standalone mode

Standalone mode has one process, one provider host, and one exclusively locked SQLite file. It is appropriate when all capacity for a scale set is available on one host and leadership failover is not required.

### Components and responsibility

```mermaid
C4Component
    title Component Diagram - Standalone Mode

    ContainerDb(sqlite, "Standalone state", "SQLite", "Durable demand and provider-independent runner lifecycle")
    System_Ext(github, "GitHub Actions", "Scale-set messages, acquisition, and JIT configuration")
    System_Ext(provider, "Docker Engine or libvirt", "Provider-visible ephemeral runners")

    Container_Boundary(daemon, "Standalone daemon") {
        Component(discovery, "Discovery reconciler", "internal/daemon", "Maintains repositories and scale-set lifecycles")
        Component(session, "Repository scale-set session", "internal/daemon", "Owns one GitHub message session and repository worker")
        Component(poller, "Message poller and handler", "internal/scaleset and internal/daemon", "Persists message effects and invokes scheduling before acknowledgement")
        Component(coordinator, "Per-scale-set coordinator", "internal/scaler", "Shares capacity across repositories and admits FIFO demand")
        Component(worker, "Repository worker", "internal/daemon", "Acquires one repository request and delegates runner operations")
        Component(reconciler, "Runner reconciler", "internal/scaler", "Recovers, starts, inspects, and cleans provider-visible runners")
    }

    Rel(discovery, session, "Creates and removes for every repository and scale set")
    Rel(session, poller, "Runs")
    Rel(poller, github, "Receives and acknowledges messages", "Scale-set session")
    Rel(poller, coordinator, "Applies demand and schedules capacity")
    Rel(coordinator, sqlite, "Stores and reads FIFO demand", "database/sql")
    Rel(coordinator, worker, "Inspects, acquires, and starts repository work")
    Rel(worker, github, "Acquires request")
    Rel(worker, reconciler, "Inspects or starts named runner")
    Rel(reconciler, github, "Generates or removes JIT runner registration")
    Rel(reconciler, provider, "Lists, starts, and destroys runners")
```

There is one coordinator for each configured scale-set name. All repository sessions for that name register a repository worker with the same coordinator. The coordinator serializes its in-memory decisions with a mutex and treats `max_runners` as one shared limit across every discovered repository.

### Durable FIFO and admission

SQLite contains one `demand` table. Each row identifies a scale set, repository, positive GitHub request ID, queue time, whether GitHub acquisition succeeded, and an auto-incremented observation order. The unique key on scale set, repository, and request ID makes redelivery harmless. Queue reads order by queue time and then observation order.

The database is configured for one open connection and protected by an adjacent process lock. A second daemon using the same path is rejected. Schema migration occurs when the store opens. A future schema version or a corrupt database prevents startup.

Before admitting new demand, the coordinator asks every repository worker for its running count and checks whether stable request-derived runners already exist. Existing named runners suppress duplicate provisioning but do not remove their queue entry; only a later GitHub started or completed message proves that demand is complete.

The coordinator then admits identifiable demand in FIFO order until shared capacity is full. For each row it:

1. Acquires the request from that repository's GitHub message session if the row is not already marked acquired.
2. Persists `acquired = true` before starting the provider instance.
3. Starts a runner whose stable name derives from the repository-qualified scale-set identity and request ID.

After identifiable FIFO work, the coordinator reconciles identity-free assigned capacity from `TotalAssignedJobs`, in deterministic repository order, without exceeding the same shared limit.

### Standalone job sequence

```mermaid
sequenceDiagram
    autonumber
    participant GH as GitHub Actions
    participant P as Message poller
    participant H as Standalone message handler
    participant C as Per-scale-set coordinator
    participant DB as SQLite demand store
    participant R as Runner reconciler
    participant X as Docker or libvirt

    GH->>P: JobAvailable message and statistics
    P->>H: Handle message
    H->>C: Apply observed and started demand
    C->>DB: Transactionally upsert and delete demand
    H->>C: Reconcile assigned capacity
    C->>DB: Read demand in FIFO order
    C->>R: Inspect running and request-named runners
    alt Capacity exists for oldest demand
        C->>GH: Acquire request for its repository
        C->>DB: Persist acquired state
        C->>R: StartNamed(request ID)
        R->>GH: Generate JIT runner configuration
        R->>X: Start ephemeral runner
    end
    H-->>P: Handling succeeded
    P->>GH: Delete message acknowledgement
    GH->>P: Later JobStarted or JobCompleted message
    P->>H: Handle completion
    H->>C: Request exact runner cleanup
    C->>DB: Atomically commit reap obligation and remove terminal demand
    P->>GH: Delete message acknowledgement
```

### Standalone recovery and lifecycle

On restart, the daemon reopens SQLite, discovers repositories, recreates workers, and schedules retained demand. Provider startup is bracketed by durable `provisioning` and `waiting_for_job` states. `JobStarted` moves the exact runner to `busy`; only waiting runners can expire. If a named runner is visible, it is not duplicated. Pre-upgrade provider instances with a GitHub registration are conservatively protected, while instances already absent from GitHub are reaped.

Each scale set applies `job_start_timeout`, defaulting to `10m`, to the persisted `waiting_since` timestamp. A reload therefore changes the lease for existing waiting runners without resetting their age. A decrease in `TotalAssignedJobs` condemns excess identity-free waiting runners immediately. When apparent capacity blocks demand, the coordinator verifies GitHub registration and condemns missing registrations without waiting for the lease.

Pending Docker and libvirt reaps are attempted before new provisioning and then once per second. Cleanup destroys only the exact owned provider instance, removes its exact GitHub registration, and retains `reap_pending` plus its capacity charge until both absences are confirmed. Confirmed cleanup immediately reschedules retained demand.

A reload first loads and validates the complete replacement configuration and App credentials. The SQLite path must be unchanged. It discovers replacement repositories and confirms their complete registration sets before stopping the old generation. For a retained scale-set name with a changed Docker or libvirt definition, the coordinator waits for any in-flight scheduling pass and then suspends provisioning. Pollers continue persisting demand while the old provider is inspected. The change proceeds only when inspection reports no running instances. Rejection releases the provisioning fence and schedules retained demand through the still-operational old generation.

After preflight succeeds, the daemon stops old pollers, closes old message sessions without deleting registrations, creates replacement coordinators and sessions over the same store, and schedules retained demand. It does not wait for provider runners to exit. Busy runners and durable lifecycle obligations survive the session-generation handover. Session-close and stale-demand cleanup failures are reported but cannot roll the process back to a generation whose pollers have already stopped. Optional pruning runs only after replacement activation succeeds. Demand belonging to removed repositories or scale sets is intentionally deleted. Durable provider lifecycle records remain until the owning configuration and host access return and confirm absence.

Normal process shutdown stops polling and closes message sessions and scale sets with bounded, concurrent cleanup. It does not destroy active runners. Repository access removal is immediate and does destroy active runners because the repository is no longer within the App's authority.

Provisioning-fence acquisition and provider inspection for a changed scale-set definition share a 30-second context. Individual session cleanup also has a 30-second context. The daemon returns systemd to ready only after a rejected preflight or the replacement reconciliation attempt finishes. A preflight failure leaves the old generation active; a post-handover cleanup failure leaves the replacement generation active and visible as incomplete cleanup in structured logs.

### Standalone deployment

```mermaid
C4Deployment
    title Deployment Diagram - Standalone Mode

    Deployment_Node(host, "Provider host", "Linux, macOS, or Windows") {
        Container(scaler, "runnerfoundry", "Go executable", "Runs discovery, scheduling, and provider reconciliation")
        ContainerDb(sqlite, "State database", "SQLite file", "Durable FIFO demand and acquired markers")
        Deployment_Node(runtime, "Execution runtime", "Docker Engine or libvirt") {
            Container(runners, "Ephemeral runners", "Containers or virtual machines", "Execute GitHub Actions jobs")
        }
    }
    System_Ext(github, "GitHub Actions", "Scale-set control plane")

    Rel(scaler, github, "Manages scale sets and runner registration", "HTTPS")
    Rel(scaler, sqlite, "Owns exclusively", "Local filesystem")
    Rel(scaler, runners, "Creates, inspects, and destroys", "Provider CLI")
    Rel(runners, github, "Poll and execute assigned workflows", "GitHub runner protocol")
```

## Clustered mode

Clustered mode separates GitHub-facing coordination from host-local execution. Every host runs the same executable, publishes its own capacity, and can execute assignments addressed to its operating-system hostname. Exactly one host holds the leader lease and manages the union of repository scale-set sessions.

### Startup and node publication

Each node performs these steps before readiness:

1. Resolve PostgreSQL connection settings and the required stable database role.
2. Inspect and automatically migrate the embedded schema upward while holding the migration advisory lock.
3. Build the GitHub App discovery client and derive the node ID from the operating-system hostname.
4. Validate each local provider and atomically publish the node heartbeat, node health, and a complete replacement set of scale-set contributions.
5. Attempt the leader advisory lock immediately; if it is held elsewhere, start a blocking contender on a dedicated direct connection.
6. Report `READY=1` after migration and publication, whether the node is leader or standby.

A contribution contains the scale-set name, GitHub-facing runner group, host-local maximum, and provider health. Every ten seconds the heartbeat worker revalidates providers and republishes the complete contribution set. A node is live for scheduling while its healthy heartbeat is no more than 30 seconds old.

Node identity is exactly the operating-system hostname and is not configurable. Hostnames must therefore be unique within one scaler cluster. Two live processes with the same hostname would publish and claim work as the same node, replacing one another's contribution rows and removing the ownership distinction required for safe local execution.

Live, healthy contributors sharing a name must agree on the runner group. Compatible maximums are summed for the GitHub-facing session, while assignment selection separately enforces each host's local maximum. Stale or unhealthy declarations remain part of the managed union but do not contribute capacity or participate in compatibility checks.

### Leadership and GitHub sessions

Leadership is a session-scoped PostgreSQL advisory lock held on a dedicated direct connection. Closing or losing that connection releases the lock. Standby nodes block on the same lock instead of polling for leadership. The heartbeat loop checks the current lease; after loss, the old leader stops its sessions without deleting GitHub scale sets and returns to contention.

When leadership is acquired, the leader reconciles GitHub sessions. It discovers repositories, reads the current aggregate scale-set statuses from PostgreSQL, audits each repository's complete registration set, and creates one atomic session group per repository. The session's advertised maximum is the current effective cluster capacity. Reconciliation runs after leadership acquisition, after a successful local configuration reload, and periodically at `github.discovery_interval`. Heartbeats continue publishing state independently of session reconciliation, and assignment eligibility reads current PostgreSQL contributions on every scheduling transaction.

An incompatible scale set receives no session. When effective capacity or a confirmed registration ID changes, reconciliation replaces the repository's complete session generation. A transient registration audit failure preserves the healthy generation and reports `leader degraded`. When the last declaration is explicitly withdrawn, the leader stops the session and deletes the GitHub scale set. Leadership loss retains the GitHub scale set so a successor can attach a replacement session.

As long as incompatible live declarations remain, the aggregate capacity is zero, the leader logs the conflict during reconciliation, and no message session is created for that scale set. Stale or unhealthy conflicting declarations do not have this effect because compatibility considers only live, healthy contributors.

### Components and concurrency

```mermaid
C4Component
    title Component Diagram - Clustered Mode

    ContainerDb(postgres, "Cluster state", "PostgreSQL", "Shared declarations, demand, assignments, migrations, and advisory locks")
    System_Ext(github, "GitHub Actions", "Repository, scale-set message, acquisition, and JIT APIs")
    System_Ext(provider, "Local Docker Engine or libvirt", "Host-local ephemeral runners")

    Container_Boundary(node, "One clustered daemon node") {
        Component(publication, "Heartbeat and publication worker", "internal/daemon", "Validates providers and replaces this node's contributions")
        Component(election, "Leader contender", "internal/daemon and internal/cluster", "Acquires and monitors the session advisory-lock lease")
        Component(leader, "Leader session manager", "internal/daemon", "Discovers repositories and maintains the union of GitHub sessions")
        Component(handler, "Cluster message handler", "internal/daemon", "Persists demand and invokes the scheduler before acknowledgement")
        Component(scheduler, "Cluster scheduler", "internal/daemon and internal/cluster", "Assigns global FIFO demand and confirms GitHub acquisition")
        Component(executor, "Local assignment executor", "internal/daemon", "Repairs starting work and starts ready assignments for this node")
        Component(reconciler, "Runner reconciler", "internal/scaler", "Uses stable runner identity around provider startup")
        Component(migrations, "Migration service", "internal/cluster", "Applies embedded reversible PostgreSQL migrations")
    }

    Rel(migrations, postgres, "Migrates under advisory lock")
    Rel(publication, provider, "Checks local provider health")
    Rel(publication, postgres, "Publishes heartbeat and contributions")
    Rel(election, postgres, "Holds or waits for leader advisory lock")
    Rel(leader, postgres, "Reads declared union and effective capacity")
    Rel(leader, github, "Maintains leader-owned scale-set sessions")
    Rel(leader, handler, "Runs message pollers")
    Rel(handler, postgres, "Enqueues and completes durable demand")
    Rel(handler, scheduler, "Requests scheduling")
    Rel(scheduler, postgres, "Creates and transitions assignments")
    Rel(scheduler, github, "Acquires selected request")
    Rel(executor, postgres, "Claims, repairs, and confirms local assignments")
    Rel(executor, reconciler, "Ensures named runner exists")
    Rel(reconciler, github, "Generates JIT configuration")
    Rel(reconciler, provider, "Inspects and starts local runner")
```

Three long-running worker loops isolate failure domains:

- The heartbeat loop publishes health and checks leader-lease liveness.
- The assignment executor loop repairs or claims host-local work once per second.
- The reconciliation loop serializes leader session reconciliation requests.

Provider starts occur outside the heartbeat path and may take up to five minutes. Coordination database calls use five-second bounds, GitHub reconciliation and acquisition use 30-second bounds, and provider inspection uses a 30-second bound. A failed leadership wait retries after two seconds. After GitHub confirms acquisition, the leader retries the database confirmation every two seconds until it succeeds or its context ends. Failed or ambiguous `starting` assignments are revisited by the one-second executor loop. These boundaries prevent a slow provider or GitHub request from consuming the node-health failover window.

### Global FIFO and contributor selection

The leader's message handler writes each positive-identity `JobAvailable` request to `scaler_demand` before scheduling. The unique scale-set, repository, and request-ID key deduplicates redelivery. FIFO order is queue time followed by a database observation sequence.

Clustered message statistics are required as part of the scale-set message contract, but they are not used to synthesize identity-free capacity. Clustered available, started, and completed entries must carry positive request IDs. A malformed or identity-free entry fails handling and remains unacknowledged instead of creating an assignment that the shared store cannot identify and recover.

For the selected scale set, PostgreSQL transactionally:

1. Completes assignments owned by expired nodes while retaining their demand.
2. Selects the oldest demand without an active assignment.
3. Selects a live, healthy, compatible contributor whose active assignment count is below that node's contribution for this scale set.
4. Inserts a `reserved` assignment for that demand and node.

The leader then records the installation and GitHub scale-set identifiers by moving the assignment to `acquiring`, asks GitHub to acquire the exact request ID, and moves it to `ready` only after GitHub confirms the requested identity. A database failure after GitHub confirmation retries only the `acquiring -> ready` transition in the same process.

### Assignment lifecycle

```mermaid
stateDiagram-v2
    [*] --> reserved: Leader assigns oldest demand to eligible node
    reserved --> acquiring: Leader records GitHub acquisition identifiers
    acquiring --> ready: GitHub confirms requested identity
    ready --> starting: Owning node atomically claims assignment
    starting --> running: Owning node observes named runner after start or repair
    running --> complete: GitHub reports completed demand

    reserved --> complete: Session unavailable, release, or node expiry
    acquiring --> complete: Acquisition failure, failed recovery, or node expiry
    ready --> complete: Owning node expires
    starting --> complete: Local scale set removed or owning node expires
    running --> complete: Demand completes or owning node expires
```

Every non-complete assignment and pending runner reap consumes the selected node's local capacity. Transitions include the expected prior state and, for host-owned transitions, the expected node ID. A stale or repeated transition fails with an assignment conflict instead of silently overwriting newer state.

The `acquiring` state is the boundary around GitHub's non-transactional acquisition API. A replacement leader may retry an interrupted acquisition only if GitHub's response confirms the requested identity. An unconfirmed acquisition never makes the assignment executable.

The `starting` state is the boundary around provider startup. Before starting, and again after an ambiguous provider error, the owning node checks for the stable request-derived runner name. A process restart repairs all of its `starting` assignments before claiming new `ready` assignments. A failed or ambiguous start remains `starting` for later repair rather than returning to the ready queue.

`JobStarted` persists the exact runner as busy without completing clustered demand. Terminal `JobCompleted` transactionally requests registered Docker or libvirt cleanup, marks active assignments complete, and removes demand before acknowledgement. An expired waiting lease makes the runner `reap_pending`, completes its assignment, and retains demand for a fresh assignment after capacity is safe. When a node expires, its active assignments are completed but their demand remains, allowing a compatible live host to receive replacement assignments.

### Clustered job sequence

```mermaid
sequenceDiagram
    autonumber
    participant GH as GitHub Actions
    participant L as Leader message handler
    participant DB as PostgreSQL
    participant S as Leader scheduler
    participant E as Selected host executor
    participant R as Runner reconciler
    participant X as Local provider

    GH->>L: JobAvailable message
    L->>DB: Upsert durable demand
    L->>S: Schedule scale set
    S->>DB: Complete expired assignments
    S->>DB: Reserve oldest demand on eligible host
    S->>DB: reserved -> acquiring with GitHub IDs
    S->>GH: Acquire exact request ID
    GH-->>S: Confirm requested identity
    S->>DB: acquiring -> ready
    L-->>GH: Acknowledge message after handler succeeds
    E->>DB: Claim local ready assignment
    DB-->>E: starting assignment
    E->>R: Check stable runner name
    alt Runner is absent
        R->>GH: Generate JIT runner configuration
        R->>X: Start ephemeral runner
        E->>R: Check stable runner name again
    end
    E->>DB: starting -> running
    GH->>L: JobStarted
    L-->>GH: Acknowledge without completing assignment
    GH->>L: JobCompleted
    L->>DB: Request exact runner reap; complete assignment and demand
    L-->>GH: Acknowledge after cleanup obligation commits
    E->>DB: Load node-owned Docker and libvirt pending reaps
    E->>X: List exact owned domain; destroy if active; list again
    E->>DB: Delete lifecycle only after confirmed absence
```

### PostgreSQL state

```mermaid
erDiagram
    SCALER_NODES ||--o{ SCALER_CONTRIBUTIONS : publishes
    SCALER_NODES ||--o{ SCALER_ASSIGNMENTS : owns
    SCALER_NODES ||--o{ SCALER_RUNNER_LIFECYCLE : owns
    SCALER_NODES ||--o{ SCALER_LIBVIRT_RUNNER_LIFECYCLE : owns
    SCALER_DEMAND ||--o{ SCALER_ASSIGNMENTS : receives

    SCALER_SCHEMA_COMPATIBILITY {
        boolean singleton PK
        integer oldest_binary_schema_generation
    }
    SCALER_NODES {
        text node_id PK
        timestamptz heartbeat_at
        boolean healthy
    }
    SCALER_CONTRIBUTIONS {
        text node_id PK, FK
        text scale_set PK
        text runner_group
        integer max_runners
        boolean healthy
    }
    SCALER_DEMAND {
        bigint demand_id PK
        bigint observation_id UK
        text scale_set
        text repository
        bigint request_id
        timestamptz queue_time
    }
    SCALER_ASSIGNMENTS {
        bigint assignment_id PK
        bigint demand_id FK
        text node_id FK
        bigint installation_id
        integer github_scale_set_id
        text state
    }
    SCALER_RUNNER_LIFECYCLE {
        text node_id PK, FK
        text scale_set PK
        text repository PK
        text provider
        text runner_name PK
        bigint request_id
        bigint assignment_id FK
        bigint github_runner_id
        text phase
        timestamptz waiting_since
        text reap_reason
    }
    SCALER_LIBVIRT_RUNNER_LIFECYCLE {
        text node_id PK, FK
        text scale_set PK
        text repository PK
        bigint request_id
        text runner_name PK
        boolean reap_requested "legacy compatibility"
    }
```

The schema enforces one active assignment per demand with a partial unique index. The generic lifecycle table records provider, exact identities, lifecycle phase, timestamps, and reap reason. Additional indexes support FIFO demand selection, active-assignment counts, waiting leases, and node-owned pending reaps. Node publication replaces all of one node's contributions in one transaction. It also removes demand only when no node declares the associated scale set; runner lifecycle records remain until confirmed provider and GitHub absence.

Ordinary state calls use the primary PostgreSQL endpoint. Migrations and advisory-lock sessions use the optional direct endpoint, or the primary endpoint when it preserves session affinity. All sessions assume the configured stable role. Login credentials may rotate, but database objects and migration metadata remain owned by the stable role.

Configuration values may be literals, environment references, or file references. References are resolved immediately before opening each new connection and are never cached. Existing connections, including a leader lease, are not recycled when a referenced file changes.

### Migrations and compatibility

Cluster startup inspects the schema and automatically migrates upward before contribution publication. A dedicated advisory lock serializes version inspection, migration, and compatibility validation among simultaneous nodes. Migrations are embedded, ordered, transactional, and reversible.

`migrate status` is read-only and reports an empty database as version zero without creating metadata. `migrate up` applies the current binary's target. Migration 003 expands lifecycle ownership to both providers, backfills the retained libvirt table, mirrors old-binary writes, and keeps new libvirt writes compatible for one release window. `migrate down --to` must be run with the currently deployed binary, acquires both migration and leader locks, and refuses rollback while a live leader or node heartbeat exists. It also refuses to discard Docker cleanup obligations that the legacy schema cannot represent. The compatibility row prevents a binary from using a schema generation below the schema's supported floor.

### Reload, failover, and shutdown

A clustered reload validates a complete replacement configuration and App key before replacing the in-memory snapshot. It cannot change PostgreSQL configuration or switch state backends. The node then republishes its complete contribution set atomically. If it is the leader, it requests immediate session reconciliation. Running assignments continue and provider configuration remains local to their owning host.

When a leader lease fails, the daemon cancels active leader reconciliation, stops message pollers, closes message sessions, retains GitHub scale sets, releases the lease, and returns to standby. A waiting node receives the advisory lock and reconstructs sessions from current discovery and PostgreSQL declarations. Durable demand and assignments do not belong to the old leader process.

On process shutdown, workers stop, any contender connection is canceled, an active leader closes its sessions without deleting scale sets, and the node publishes itself unhealthy. If that final publication fails, its heartbeat still expires after the node lifetime.

### Cluster deployment

```mermaid
C4Deployment
    title Deployment Diagram - Clustered Mode

    Deployment_Node(host_a, "Provider host A", "Operating-system host") {
        Container(scaler_a, "runnerfoundry node A", "Go executable", "Publishes capacity, contends for leadership, and executes local assignments")
        Deployment_Node(runtime_a, "Local provider A", "Docker Engine or libvirt") {
            Container(runners_a, "Ephemeral runners A", "Containers or virtual machines", "Execute assignments owned by host A")
        }
    }
    Deployment_Node(host_b, "Provider host B", "Operating-system host") {
        Container(scaler_b, "runnerfoundry node B", "Go executable", "Publishes capacity, contends for leadership, and executes local assignments")
        Deployment_Node(runtime_b, "Local provider B", "Docker Engine or libvirt") {
            Container(runners_b, "Ephemeral runners B", "Containers or virtual machines", "Execute assignments owned by host B")
        }
    }
    Deployment_Node(database, "PostgreSQL service", "Session-affine PostgreSQL") {
        ContainerDb(state, "runnerfoundry schema", "PostgreSQL", "Shared nodes, contributions, demand, assignments, and locks")
    }
    System_Ext(github, "GitHub Actions", "Scale-set control plane")

    Rel(scaler_a, state, "Publishes, schedules if leader, and executes assignments", "pgx")
    Rel(scaler_b, state, "Publishes, schedules if leader, and executes assignments", "pgx")
    Rel(scaler_a, github, "Manages sessions only while leader; registers local runners", "HTTPS")
    Rel(scaler_b, github, "Manages sessions only while leader; registers local runners", "HTTPS")
    Rel(scaler_a, runners_a, "Creates and inspects")
    Rel(scaler_b, runners_b, "Creates and inspects")
    Rel(runners_a, github, "Execute workflows", "GitHub runner protocol")
    Rel(runners_b, github, "Execute workflows", "GitHub runner protocol")
```

## Runner execution boundary

The scheduling layers do not know Docker or libvirt command details. They depend on a repository worker or assignment executor, which delegates provider-visible operations to `scaler.Reconciler`. The reconciler depends on two interfaces:

- `Provider` lists, starts, and destroys instances identified by scale-set and runner names.
- `JITConfigGenerator` creates GitHub runner registrations and removes a generated registration when provider startup fails.

This boundary centralizes runner capacity checks, lifecycle registration before provider startup, bounded waiting, completed-runner cleanup, stable identity, JIT cleanup, and provider-independent recovery.

```mermaid
classDiagram
    class Poller {
        +MessageClient Client
        +MessageHandler Handler
        +int MaxCapacity
        +PollOnce(ctx) error
    }
    class MessageHandler {
        <<interface>>
        +HandleMessage(ctx, message) error
    }
    class CoordinatedScaleSetHandler {
        +Coordinator Coordinator
        +HandleMessage(ctx, message) error
    }
    class ClusterMessageHandler {
        +ClusterSchedulingState Store
        +ClusterScheduler Scheduler
        +HandleMessage(ctx, message) error
    }
    class Coordinator {
        -int maxRunners
        -DemandStore store
        -map workers
        +Apply(ctx, observed, removed) error
        +Schedule(ctx) error
        +ReconcileAssigned(ctx, repository, count)
    }
    class DemandStore {
        <<interface>>
        +Apply(ctx, scaleSet, observed, removed) error
        +Ordered(ctx, scaleSet) QueuedDemand[]
    }
    class RepositoryWorker {
        <<interface>>
        +Running(ctx) int
        +Exists(ctx, requestID) bool
        +Acquire(ctx, requestID) error
        +StartOne(ctx, requestID) error
    }
    class ClusterScheduler {
        -ClusterSchedulingState store
        +schedule(ctx, scaleSet) error
    }
    class ClusterSchedulingState {
        <<interface>>
        +AssignOldest(ctx, scaleSet, now, liveFor) Assignment
        +BeginAcquisition(ctx, assignmentID, installationID, scaleSetID) error
        +ConfirmAcquisition(ctx, assignmentID) error
    }
    class Reconciler {
        +ScaleSetRuntime ScaleSet
        +Running(ctx) int
        +Exists(ctx, identifier) bool
        +StartNamed(ctx, identifier) error
        +Drain(ctx) int
        +DestroyActive(ctx) error
    }
    class ScaleSetRuntime {
        +string Name
        +int MaxRunners
        +Provider Provider
        +JITConfigGenerator JITConfig
    }
    class Provider {
        <<interface>>
        +List(ctx, scaleSetName) RunnerInstance[]
        +Start(ctx, request) error
        +Destroy(ctx, instance) error
    }
    class JITConfigGenerator {
        <<interface>>
        +Generate(ctx, runnerName) JITRunnerConfig
        +Remove(ctx, runner) error
    }
    class DockerComposeProvider
    class LibvirtProvider
    class ScalesetJITGenerator

    Poller --> MessageHandler : calls before acknowledgement
    MessageHandler <|.. CoordinatedScaleSetHandler
    MessageHandler <|.. ClusterMessageHandler
    CoordinatedScaleSetHandler --> Coordinator
    Coordinator --> DemandStore
    Coordinator --> RepositoryWorker
    RepositoryWorker --> Reconciler
    ClusterMessageHandler --> ClusterScheduler
    ClusterScheduler --> ClusterSchedulingState
    Reconciler *-- ScaleSetRuntime
    ScaleSetRuntime --> Provider
    ScaleSetRuntime --> JITConfigGenerator
    Provider <|.. DockerComposeProvider
    Provider <|.. LibvirtProvider
    JITConfigGenerator <|.. ScalesetJITGenerator
```

### Stable runner identity

Provider runner names include the repository-qualified scale-set identity. Request-bound runners also include a compact representation of the positive GitHub request ID. This prevents repositories sharing one configured scale-set name from colliding on the same provider host and gives restart recovery a deterministic lookup key.

When generating JIT configuration, an existing GitHub runner with the same name is treated as stale. The scaler finds and removes it, then retries JIT generation. If provider startup fails after JIT generation, the scaler removes the generated GitHub runner registration so a later retry does not inherit a false conflict.

### Docker Compose provider

The Docker provider treats the configured Compose file and service as the operator-owned runner template. For each runner it invokes `docker compose run` in detached, auto-remove mode, adds scaler ownership labels, sets the stable container name, and injects the encoded JIT configuration through `ACTIONS_RUNNER_INPUT_JITCONFIG`.

Listing uses Docker labels to find only scaler-owned containers for the local scale-set identity. Running and completed states are translated into the provider-independent runner model. Destruction removes the selected owned container. The scaler does not own image, volume, network, command, user, or other Compose service configuration.

### libvirt provider

The libvirt provider treats the configured domain XML as an operator-owned template. It renders a unique domain name, scaler metadata, and the JIT configuration into SMBIOS OEM strings, then starts a transient domain through `virsh create`. RunnerFoundry persists the runner lifecycle before that command.

Listing finds domains carrying the scaler metadata, inspects their state, and translates them into the provider-independent runner model. Terminal reaping matches the exact runner metadata, force-stops any active state, and lists again. The durable obligation is removed only when that second inspection, or an initial inspection, confirms absence. Provider validation checks both XML rendering and libvirt connectivity. libvirt execution is unavailable on Windows.

## Readiness, observability, and security boundaries

### Readiness and operating status

Standalone readiness means configuration, credentials, local state, and initial composition are valid. Clustered readiness additionally means PostgreSQL migration and initial contribution publication succeeded. Readiness does not mean GitHub discovery or reconciliation succeeded.

Clustered nodes report a separate systemd status:

- `standby` while another node owns leadership.
- `leader reconciling` while rebuilding GitHub-facing sessions.
- `leader operational` after successful reconciliation.
- `leader degraded` after a reconciliation failure.

Structured logs describe lifecycle actions and include non-secret repository, scale-set, node, request, assignment, and operation identifiers. Debug logging adds individual discovery, polling, scheduling, recovery, migration, reload, provider, and cleanup actions.

### Credential and URL handling

The GitHub App private key is read from its configured file and held in process memory. Plaintext private keys in YAML and legacy personal-access-token fields are rejected. JIT configuration is passed directly from GitHub to the selected provider and is not persisted in SQLite or PostgreSQL.

PostgreSQL credentials and connection references are resolved only when opening a connection. Diagnostic errors deliberately omit resolved connection values, stable role names, reference paths, generated connection strings, SQL bodies, raw driver connection errors, and JIT secrets.

GitHub repository URLs must use HTTPS. GitHub-hosted Actions service URLs must use the trusted `actions.githubusercontent.com` suffix and default HTTPS port. GitHub Enterprise service URLs must remain on the repository origin. Cross-origin redirects are rejected.

## Recovery guarantees and limits

The architecture protects the following invariants:

| Invariant | Mechanism | Remaining limit |
| --- | --- | --- |
| A handled message is not silently lost before durable effects | Poller acknowledges only after its handler succeeds | An acknowledgement failure can redeliver an already handled message |
| Positive-identity demand survives process restart | SQLite or PostgreSQL stores demand before acknowledgement | Identity-free assigned work relies on GitHub statistics in standalone mode |
| Redelivery does not create duplicate demand | Unique demand identity and idempotent upsert | External APIs and providers are still separate transaction domains |
| Capacity is not exceeded | Standalone counts provider-visible runners and pending reaps; PostgreSQL counts active assignments and pending reaps per contributor | Pending cleanup deliberately retains capacity while provider state is ambiguous |
| Accepted runner cleanup is not forgotten | Provider-independent lifecycle intent commits before `JobCompleted` acknowledgement and retries until provider and GitHub absence are confirmed | The owning host must regain its configuration and provider access; GitHub must respond |
| Waiting capacity cannot strand demand forever | `job_start_timeout`, assigned-count cancellation, and GitHub registration checks move unusable waiting runners to durable cleanup | Busy runners are protected; cleanup retains capacity while absence is ambiguous |
| A discovered repository cannot silently lose part of its configured registration set | Discovery-cadence grouped audits retain successful creations and activate only complete repository session generations | Active generations remain in use while an audit result is inconclusive |
| Interrupted provider starts do not blindly duplicate runners | Stable runner name is checked before and after start and during repair | A provider that cannot report accurate identity cannot provide this guarantee |
| Cluster leadership has one active owner | Session advisory lock on a dedicated connection | PostgreSQL availability is required for leadership and shared scheduling |
| Node loss does not lose queued demand | Expired-node assignments complete while demand remains | Work may be retried on another host after the original host becomes unreachable |
| Credentials do not enter durable state or diagnostic output | Files and environment are resolved at connection time; secrets are filtered | Existing database connections are not forcibly rotated |

The scaler deliberately favors retained or retried demand over silent loss. GitHub acquisition, database transitions, and provider startup cannot be atomic. The persisted `acquiring` and `starting` boundaries, confirmation checks, stable identities, and idempotent completion rules make those gaps observable and repairable.
