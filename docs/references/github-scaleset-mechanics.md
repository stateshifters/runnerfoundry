# GitHub runner scale-set mechanics

This reference explains the GitHub runner scale-set protocol that the
[GitHub adapter](../github-adapter.md) consumes. It describes GitHub's message and assignment
mechanics, not RunnerFoundry's durable job lifecycle. The GitHub adapter remains the owner of
how these upstream observations are translated into RunnerFoundry state.

## RunnerFoundry's scale-set scope

RunnerFoundry registers one repository-scoped GitHub runner scale set for each repository and
configured Runner Pool pair. One message session therefore observes demand for exactly one such
pair:

```text
GitHub App installation (authentication and repository discovery)
└── repository
    └── Runner Pool / GitHub runner scale set
        └── message session and scale-set statistics
```

The installation can expose multiple repositories, but it is not a demand boundary. A runner
group controls which workflows may target the scale set, but it is not a demand boundary either.
The execution provider supplies runner capacity after RunnerFoundry has accepted demand; it does
not participate in GitHub's job-matching or assignment decisions.

The upstream scale-set library also supports organization- and enterprise-scoped registrations.
Those modes do not change the repository-scoped model used by RunnerFoundry.

## Matching, demand, and assignment

For one repository and Runner Pool scale set, the normal sequence is:

1. A workflow job targets the scale-set label. GitHub applies the label and runner-group policies
   and matches the job to the scale set.
2. GitHub can publish a `JobAvailable` message offering that job to the scale-set client. The
   client submits the offered request IDs to `AcquireJobs`; the response identifies which offers
   it acquired.
3. Poll responses report the scale set's current demand in
   `statistics.TotalAssignedJobs`. This is the number of jobs currently assigned to the scale
   set, including jobs waiting for a runner and jobs already running.
4. RunnerFoundry follows its internal job lifecycle to provision runner capacity. From GitHub's
   perspective, each registered runner belongs to the scale set rather than to a particular
   pending GitHub job.
5. GitHub assigns a pending job to any idle runner registered in that scale set.
6. `JobStarted` and `JobCompleted` messages can then describe execution on the selected runner.

At the GitHub protocol boundary, the important handoff is therefore scale-set demand, not a
binding between one pending GitHub job and one registered runner. RunnerFoundry creates capacity
for the repository and Runner Pool; GitHub chooses which pending job an idle scale-set runner
receives. This does not change RunnerFoundry's source-neutral [internal job
lifecycle](../PRD.md#job-lifecycle).

## What a poll response contains

A `RunnerScaleSetMessage` combines two different kinds of information:

| Part | Meaning | Appropriate use |
| --- | --- | --- |
| `Statistics` | A current aggregate view of jobs and runners in the scale set | Scaling and capacity decisions |
| Lifecycle message arrays | A bounded batch of individual events | Acquisition, lifecycle observation, metrics, and logging |

The lifecycle arrays contain these message types:

| Message | Meaning |
| --- | --- |
| `JobAvailable` | GitHub is offering a job for acquisition by this scale set. It carries an acquisition URL and a runner request ID. |
| `JobAssigned` | GitHub assigned a workflow job to the scale set. It is lifecycle history, not an acquisition, scaling, or RunnerFoundry queue-ingestion input, and it does not identify a particular provisioned runner. |
| `JobStarted` | A runner acquired and started the job. The payload includes the runner ID and name. |
| `JobCompleted` | The attempt completed or its assignment was cancelled. The payload includes the result and runner identity fields. |

These arrays are not a complete job ledger. A response contains at most 50 lifecycle messages, so
a backlog can be truncated. Messages can also be redelivered or repeated across assignment
attempts. Their count cannot answer "how many runners does this scale set need?"

## The authoritative scaling signal

`statistics.TotalAssignedJobs` is the scale-set library's scaling signal. It includes both jobs
waiting for a runner and jobs already running, so it is always greater than or equal to
`TotalRunningJobs`.

Scale from that current total, not from:

- the number of `JobAvailable` or `JobAssigned` entries in one response;
- the net history of `JobAssigned`, `JobStarted`, and `JobCompleted` entries;
- whether a lifecycle message has a positive `RunnerRequestID`.

The client also reports the scale set's maximum capacity with each poll. GitHub uses that capacity
when assigning work so that it does not intentionally build a backlog beyond what the scale set
reports it can provide.

## Runner request IDs

`RunnerRequestID` has a specific role in the `JobAvailable` acquisition exchange. The scale-set
client collects request IDs from offered jobs and passes them to `AcquireJobs`. RunnerFoundry can
only attempt that acquisition for an offer with a usable positive request ID.

The upstream payload type also embeds `RunnerRequestID` in the other lifecycle message types. That
shared field does not make request-ID positivity a universal validity rule. `JobAssigned` is not a
queue-ingestion input regardless of whether its request ID is positive, zero, or unusable. A
`JobStarted` or `JobCompleted` event with request ID zero can be correlated by its RunnerFoundry
runner name; without either identity, it cannot change RunnerFoundry lifecycle state. None of these
cases invalidates the response statistics. Scaling is governed by `TotalAssignedJobs`, not by a
request ID.

`JobID`, workflow metadata, runner identity, and timestamps help correlate or observe lifecycle
events. They do not replace the aggregate scaling signal.

## Polling, cursors, and acknowledgement

`GetMessage` is a long poll:

- GitHub returns immediately when messages are available.
- Otherwise, the request waits for approximately 50 seconds.
- If no message arrives, GitHub returns HTTP `202`; the Go client represents that as a nil message
  and nil error. The caller polls again.

The caller passes the last processed message ID as the cursor. Passing zero, or omitting the
cursor, starts from the first available message and can reprocess earlier messages.

After processing a message, the client calls `DeleteMessage` to acknowledge it. An unacknowledged
message is delivered again on a later poll. Consumers must therefore expect redelivery and make
their handling idempotent.

## Cancellation and reassignment

An assignment can expire before a runner acquires the job. GitHub then emits `JobCompleted` with a
`canceled` result, requeues the workflow job, and can assign it again. The scale-set library notes
that this can happen up to three times with increasing delays.

Each attempt produces new lifecycle messages even though the messages describe the same workflow
job. This is another reason not to derive demand by counting or balancing message history:
`TotalAssignedJobs` describes the current scale-set state directly.

## Terminology

| Term | Meaning in RunnerFoundry |
| --- | --- |
| GitHub App installation | Authentication and repository-discovery scope; not a demand total |
| Repository | One half of RunnerFoundry's scale-set session identity |
| Runner Pool | RunnerFoundry configuration whose name is published as the GitHub scale-set label |
| Runner scale set | The GitHub registration for one repository and Runner Pool |
| Message session | Long-polling session for one registered runner scale set |
| Runner group | GitHub access policy applied during job matching; not the demand boundary |
| `TotalAssignedJobs` | Current number of jobs assigned to the scale set, waiting or running |
| `RunnerRequestID` | Acquisition handle used for a `JobAvailable` offer; not the scaling total |

## Upstream sources

This reference follows the version of `github.com/actions/scaleset` pinned by RunnerFoundry at
commit `cb0405b2d874`:

- [Scale-set README](https://github.com/actions/scaleset/blob/cb0405b2d874/README.md)
- [Message and statistics types](https://github.com/actions/scaleset/blob/cb0405b2d874/types.go)
- [Listener message handling](https://github.com/actions/scaleset/blob/cb0405b2d874/listener/listener.go)
