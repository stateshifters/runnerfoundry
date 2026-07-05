# Github Scaler

This project is a Github Runner automation plane built on top of [Github Scaleset](https://github.com/actions/scaleset)

The main purpose is to allow autoscaling of self-hosted docker and libvirt managed runners for Github Actions.

We're currently working on the very first version of this project.

## Version 1

### Features

- The scaler is for Linux only for now
- Define scale-sets in a simple YAML file
- Runners can be docker containers or libvirt VMs
- Runners can be automatically scaled up and down based on the number of jobs in the queue up to a maximum number of runners defined in the scale-set
- Runners automatically get destroyed when they are no longer needed

## Assumptions

- All containers and VMs are ephemeral and will be destroyed when they are no longer needed
- All containers and VMs are set up to register their runners with the Github API and will be automatically unregistered when they are finished.
- Docker containers will quit once they have finished their job - and since we run them with --rm, they will be automatically destroyed
- Libvirt VMs do NOT stop themselves when they have finished their job, so we will need to destroy them manually. This is done by the Scaleset API when the job is completed
- A VM/Container is assigned to one job.
