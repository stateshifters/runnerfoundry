# 2. Use GitHub App installations and shared runner capacity

Date: 2026-07-11

## Status

Superseded by [3. Persist shared queue state in SQLite](0003-persist-shared-queue-state-in-sqlite.md)

Amended by [6. Continuously reconcile repository scale-set registration](0006-continuously-reconcile-repository-scale-set-registration.md)

## Context

The daemon authenticated with a personal access token and required every repository to be repeated in local configuration. PAT ownership tied the service to a user, while repository selection duplicated the access policy already maintained by GitHub App installations.

Creating one independent scaler per repository also multiplied `max_runners` by the repository count. A single provider host needs a predictable capacity ceiling even when the App is installed across many accounts. GitHub scale-set messages identify individual jobs and include their queue times, but GitHub retains control of exact job assignment.

The project intentionally avoids a local database. Repository access changes must therefore be discovered periodically, and queue ordering cannot survive process restarts.

## Decision

We will authenticate exclusively as a GitHub App. The daemon will discover every installation owned by that App and every repository exposed by each installation. GitHub installation repository selection will be the sole repository opt-in boundary.

Each configured scale-set definition will have one shared `max_runners` budget across all installations and repositories. The daemon will order observed jobs by GitHub queue time and give the next available runner to the repository containing the oldest job. GitHub will continue to choose the exact job assigned within that repository.

Discovery will run every five minutes by default, with a configurable minimum of one minute. A transient installation failure will retain that installation's last-known repositories. Revoked repository access will trigger immediate local runner destruction and best-effort scale-set unregistration.

## Consequences

Authentication is independent of a human account, and GitHub becomes the authoritative repository-access policy. Adding or removing installation access no longer requires editing every scale-set definition.

Capacity remains bounded as repository and installation counts grow. FIFO admission is fair across the App as far as the scale-set protocol permits, but GitHub may select a different compatible job within the chosen repository.

PAT configuration and explicit repository lists are breaking changes. Operators must create an App, install it, store its private key, and migrate the YAML configuration.

Queue state remains in memory. A restart recovers runners and demand from GitHub but resets ordering among waiting jobs. Revoking repository access can interrupt active jobs because cleanup is immediate.
