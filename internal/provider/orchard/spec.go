package orchard

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"net/url"
	"slices"
	"strings"

	"github.com/cirruslabs/orchard/pkg/resource/v1"
)

func normalizeURL(address string) (string, error) {
	parsed, err := url.Parse(address)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", errors.New("orchard controller URL must be an absolute HTTP or HTTPS URL without credentials, query or fragment")
	}
	parsed.Host = strings.ToLower(parsed.Host)
	if (parsed.Scheme == "https" && parsed.Port() == "443") || (parsed.Scheme == "http" && parsed.Port() == "80") {
		parsed.Host = parsed.Hostname()
		if strings.Contains(parsed.Host, ":") {
			parsed.Host = "[" + parsed.Host + "]"
		}
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawPath = ""
	return parsed.String(), nil
}

func validateSpec(content []byte) (v1.VM, error) {
	if err := validateSpecFields(content); err != nil {
		return v1.VM{}, err
	}
	vm, err := decodeSpec(content)
	if err != nil {
		return vm, err
	}
	if err := validateRuntime(vm); err != nil {
		return vm, err
	}
	if err := validatePolicies(vm); err != nil {
		return vm, err
	}
	if err := validateNativeFields(vm); err != nil {
		return vm, err
	}
	if err := validateDirectories(vm.HostDirs); err != nil {
		return vm, err
	}
	return vm, nil
}

func validateSpecFields(content []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(content, &fields); err != nil || fields == nil {
		return errors.New("orchard VM specification must be a JSON object")
	}
	allowed := strings.Fields("image imagePullPolicy cpu memory diskSize net-bridged headless nested audio clipboard os arch runtime hostProcesses endpoints net-softnet netSoftnet netSoftnetAllow netSoftnetBlock suspendable usbAccessories powerState username password startup_script restart_policy randomSerial resources labels hostDirs")
	for field := range fields {
		if !slices.Contains(allowed, field) {
			return errors.New("orchard VM specification contains an unknown or server-owned field")
		}
	}
	return nil
}

func validateRuntime(vm v1.VM) error {
	if vm.Image == "" || (vm.Runtime != "" && vm.Runtime != v1.RuntimeTart) || (vm.OS != "" && vm.OS != v1.OSDarwin) || (vm.Arch != "" && vm.Arch != v1.ArchitectureARM64) || (vm.PowerState != "" && vm.PowerState != v1.PowerStateRunning) {
		return errors.New("orchard requires an image and a running Tart Darwin ARM64 VM")
	}
	return nil
}

func validatePolicies(vm v1.VM) error {
	if vm.RestartPolicy != "" {
		if _, err := v1.NewRestartPolicyFromString(string(vm.RestartPolicy)); err != nil {
			return errors.New("invalid Orchard restart policy")
		}
	}
	if vm.ImagePullPolicy != "" {
		if _, err := v1.NewImagePullPolicyFromString(string(vm.ImagePullPolicy)); err != nil {
			return errors.New("invalid Orchard image pull policy")
		}
	}
	return nil
}

func validateNativeFields(vm v1.VM) error {
	if vm.Runtime == "" {
		vm.Runtime = v1.RuntimeTart
	}
	if err := vm.Validate(); err != nil {
		return errors.New("invalid Orchard VM specification")
	}
	if err := v1.ValidateEndpoints(vm.Endpoints); err != nil {
		return errors.New("invalid Orchard VM endpoints")
	}
	if err := v1.ValidateHostProcesses(vm.HostProcesses); err != nil {
		return errors.New("invalid Orchard VM host processes")
	}
	return nil
}

func validateDirectories(directories []v1.HostDir) error {
	for _, directory := range directories {
		if directory.Name == "" || directory.Path == "" {
			return errors.New("orchard VM host directories require a name and path")
		}
	}
	return nil
}

func cloneSpec(vm v1.VM) v1.VM {
	vm.Resources = vm.Resources.Copy()
	vm.Labels = vm.Labels.Copy()
	vm.HostDirs = slices.Clone(vm.HostDirs)
	vm.NetSoftnetAllow = slices.Clone(vm.NetSoftnetAllow)
	vm.NetSoftnetBlock = slices.Clone(vm.NetSoftnetBlock)
	vm.HostProcesses = slices.Clone(vm.HostProcesses)
	for index := range vm.HostProcesses {
		vm.HostProcesses[index].Args = slices.Clone(vm.HostProcesses[index].Args)
		vm.HostProcesses[index].Env = maps.Clone(vm.HostProcesses[index].Env)
	}
	vm.Endpoints = slices.Clone(vm.Endpoints)
	for index := range vm.Endpoints {
		target := *vm.Endpoints[index].Target.VM
		vm.Endpoints[index].Target.VM = &target
		if vm.Endpoints[index].WorkerPortRange != nil {
			portRange := *vm.Endpoints[index].WorkerPortRange
			vm.Endpoints[index].WorkerPortRange = &portRange
		}
	}
	if vm.StartupScript != nil {
		script := *vm.StartupScript
		script.Env = maps.Clone(script.Env)
		vm.StartupScript = &script
	}
	return vm
}

func decodeSpec(content []byte) (v1.VM, error) {
	var vm v1.VM
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&vm); err != nil {
		return vm, errors.New("invalid Orchard VM specification")
	}
	return vm, nil
}
