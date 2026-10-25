# Install the Orchard guest example

Prepare the base, isolated `runner` account, unregistered runner template and agent replacement
using the [Orchard guide](../../docs/providers/orchard.md#configure-the-macos-guest).
Copy this directory's launcher and two plists into the guest. From that directory in the guest, install them:

```sh
sudo install -d -o root -g wheel -m 0755 /usr/local/sbin
sudo install -o root -g wheel -m 0755 runnerfoundry-jit-launcher /usr/local/sbin/runnerfoundry-jit-launcher
sudo install -o root -g wheel -m 0644 com.runnerfoundry.runner.plist /Library/LaunchDaemons/com.runnerfoundry.runner.plist
sudo install -o root -g wheel -m 0644 com.runnerfoundry.orchard-agent.plist /Library/LaunchDaemons/com.runnerfoundry.orchard-agent.plist
sudo shutdown -h now
```

Copy `vm.json` into the RunnerFoundry working directory as `macos-vm.json` on the member host.
Continue with the [Orchard controller setup](../../docs/providers/orchard.md#start-the-orchard-controller), then follow the guide through the first job.
