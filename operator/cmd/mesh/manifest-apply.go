// Copyright 2019 Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mesh

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"istio.io/api/operator/v1alpha1"
	iopv1alpha1 "istio.io/istio/operator/pkg/apis/istio/v1alpha1"
	"istio.io/istio/operator/pkg/kubectlcmd"
	"istio.io/istio/operator/pkg/manifest"
	"istio.io/istio/operator/pkg/name"
	"istio.io/istio/operator/pkg/translate"
	"istio.io/istio/operator/version"
)

const (
	// installedSpecCRPrefix is the prefix of any IstioOperator CR stored in the cluster that is a copy of the CR used
	// in the last manifest apply operation.
	installedSpecCRPrefix = "installed-state"
)

type manifestApplyArgs struct {
	// inFilenames is an array of paths to the input IstioOperator CR files.
	inFilenames []string
	// kubeConfigPath is the path to kube config file.
	kubeConfigPath string
	// context is the cluster context in the kube config
	context string
	// readinessTimeout is maximum time to wait for all Istio resources to be ready.
	readinessTimeout time.Duration
	// wait is flag that indicates whether to wait resources ready before exiting.
	wait bool
	// skipConfirmation determines whether the user is prompted for confirmation.
	// If set to true, the user is not prompted and a Yes response is assumed in all cases.
	skipConfirmation bool
	// force proceeds even if there are validation errors
	force bool
	// set is a string with element format "path=value" where path is an IstioOperator path and the value is a
	// value to set the node at that path to.
	set []string
	// path where to save and store the certificates
	certDir string
}

type runApplyManifestArgs struct {
	set              []string
	inFilenames      []string
	force            bool
	dryRun           bool
	verbose          bool
	kubeConfigPath   string
	context          string
	wait             bool
	readinessTimeout time.Duration
	l                *Logger
	certDir          string
}

func addManifestApplyFlags(cmd *cobra.Command, args *manifestApplyArgs) {
	cmd.PersistentFlags().StringSliceVarP(&args.inFilenames, "filename", "f", nil, filenameFlagHelpStr)
	cmd.PersistentFlags().StringVarP(&args.kubeConfigPath, "kubeconfig", "c", "", "Path to kube config")
	cmd.PersistentFlags().StringVar(&args.context, "context", "", "The name of the kubeconfig context to use")
	cmd.PersistentFlags().BoolVarP(&args.skipConfirmation, "skip-confirmation", "y", false, skipConfirmationFlagHelpStr)
	cmd.PersistentFlags().BoolVar(&args.force, "force", false, "Proceed even with validation errors")
	cmd.PersistentFlags().DurationVar(&args.readinessTimeout, "readiness-timeout", 300*time.Second, "Maximum seconds to wait for all Istio resources to be ready."+
		" The --wait flag must be set for this flag to apply")
	cmd.PersistentFlags().BoolVarP(&args.wait, "wait", "w", false, "Wait, if set will wait until all Pods, Services, and minimum number of Pods "+
		"of a Deployment are in a ready state before the command exits. It will wait for a maximum duration of --readiness-timeout seconds")
	cmd.PersistentFlags().StringArrayVarP(&args.set, "set", "s", nil, SetFlagHelpStr)
	cmd.PersistentFlags().StringVarP(&args.certDir, "cert-dir", "t", "", "The path where to read plugged-in external certs "+
		"or to store the generated self-signed root and intermediate certificates and keys")
}

func manifestApplyCmd(rootArgs *rootArgs, maArgs *manifestApplyArgs) *cobra.Command {
	return &cobra.Command{
		Use:   "apply",
		Short: "Applies an Istio manifest, installing or reconfiguring Istio on a cluster.",
		Long:  "The apply subcommand generates an Istio install manifest and applies it to a cluster.",
		// nolint: lll
		Example: `  # Apply a default Istio installation
  istioctl manifest apply

  # Enable grafana dashboard
  istioctl manifest apply --set values.grafana.enabled=true

 # plugin in existing CA Certificates from cert-dir /samples/certs
 # https://preliminary.istio.io/docs/tasks/security/plugin-ca-cert/
  istioctl manifest apply --cert-dir=/samples/certs

 # generate root certs for myCluster, two intermediate CA certs, keys and save them under /etc/istio/certs if there is no
 # required plugged-in external certs under /etc/istio/certs
  istioctl manifest apply --cert-dir=/etc/istio/certs --set values.global.clusterID=myCluster

  # Generate the demo profile and don't wait for confirmation
  istioctl manifest apply --set profile=demo --skip-confirmation

  # To override a setting that includes dots, escape them with a backslash (\).  Your shell may require enclosing quotes.
  istioctl manifest apply --set "values.sidecarInjectorWebhook.injectedAnnotations.container\.apparmor\.security\.beta\.kubernetes\.io/istio-proxy=runtime/default"
`,
		Args: cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runApplyCmd(cmd, rootArgs, maArgs)
		}}
}

// InstallCmd in an alias for manifest apply.
func InstallCmd() *cobra.Command {
	rootArgs := &rootArgs{}
	macArgs := &manifestApplyArgs{}

	mac := &cobra.Command{
		Use:   "install",
		Short: "Applies an Istio manifest, installing or reconfiguring Istio on a cluster.",
		Long:  "The install generates an Istio install manifest and applies it to a cluster.",
		// nolint: lll
		Example: `  # Apply a default Istio installation
  istioctl install

  # Enable grafana dashboard
  istioctl install --set values.grafana.enabled=true

  # Generate the demo profile and don't wait for confirmation
  istioctl install --set profile=demo --skip-confirmation

 # plugin in existing CA Certificates from cert-dir /samples/certs
 # https://preliminary.istio.io/docs/tasks/security/plugin-ca-cert/
  istioctl install --cert-dir=/samples/certs

 # generate root certs for myCluster, two intermediate CA certs, keys and save them under /etc/istio/certs if there is no
 # required plugged-in external certs under /etc/istio/certs
  istioctl install --cert-dir=/etc/istio/certs --set values.global.clusterID=myCluster

  # To override a setting that includes dots, escape them with a backslash (\).  Your shell may require enclosing quotes.
  istioctl install --set "values.sidecarInjectorWebhook.injectedAnnotations.container\.apparmor\.security\.beta\.kubernetes\.io/istio-proxy=runtime/default"
`,
		Args: cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runApplyCmd(cmd, rootArgs, macArgs)
		}}

	addFlags(mac, rootArgs)
	addManifestApplyFlags(mac, macArgs)
	return mac
}

func runApplyCmd(cmd *cobra.Command, rootArgs *rootArgs, maArgs *manifestApplyArgs) error {
	l := NewLogger(rootArgs.logToStdErr, cmd.OutOrStdout(), cmd.ErrOrStderr())
	// Warn users if they use `manifest apply` without any config args.
	if len(maArgs.inFilenames) == 0 && len(maArgs.set) == 0 && !rootArgs.dryRun && !maArgs.skipConfirmation {
		if !confirm("This will install the default Istio profile into the cluster. Proceed? (y/N)", cmd.OutOrStdout()) {
			cmd.Print("Cancelled.\n")
			os.Exit(1)
		}
	}
	if err := configLogs(rootArgs.logToStdErr); err != nil {
		return fmt.Errorf("could not configure logs: %s", err)
	}
	args := &runApplyManifestArgs{
		set:              maArgs.set,
		inFilenames:      maArgs.inFilenames,
		force:            maArgs.force,
		dryRun:           rootArgs.dryRun,
		verbose:          rootArgs.verbose,
		kubeConfigPath:   maArgs.kubeConfigPath,
		context:          maArgs.context,
		wait:             maArgs.wait,
		readinessTimeout: maArgs.readinessTimeout,
		l:                l,
		certDir:          maArgs.certDir,
	}

	if err := ApplyManifests(args); err != nil {
		return fmt.Errorf("failed to apply manifests: %v", err)
	}

	return nil
}

// ApplyManifests generates manifests from the given input files and --set flag overlays and applies them to the
// cluster. See GenManifests for more description of the manifest generation process.
//  force   validation warnings are written to logger but command is not aborted
//  dryRun  all operations are done but nothing is written
//  verbose full manifests are output
//  wait    block until Services and Deployments are ready, or timeout after waitTimeout
func ApplyManifests(args *runApplyManifestArgs) error {

	ysf, err := yamlFromSetFlags(args.set, args.force, args.l)
	if err != nil {
		return err
	}

	kubeconfig, err := manifest.InitK8SRestClient(args.kubeConfigPath, args.context)
	if err != nil {
		return err
	}
	manifests, iops, err := GenManifests(args.inFilenames, ysf, args.force, kubeconfig, args.l)
	if err != nil {
		return fmt.Errorf("failed to generate manifest: %v", err)
	}
	opts := &kubectlcmd.Options{
		DryRun:      args.dryRun,
		Verbose:     args.verbose,
		Wait:        args.wait,
		WaitTimeout: args.readinessTimeout,
		Kubeconfig:  args.kubeConfigPath,
		Context:     args.context,
	}

	for cn := range name.DeprecatedComponentNamesMap {
		manifests[cn] = append(manifests[cn], fmt.Sprintf("# %s component has been deprecated.\n", cn))
	}

	out, err := manifest.ApplyAll(manifests, version.OperatorBinaryVersion, iops, opts, args.certDir)
	if err != nil {
		return fmt.Errorf("failed to apply manifest with kubectl client: %v", err)
	}
	gotError := false

	for cn := range manifests {
		if out[cn].Err != nil {
			cs := fmt.Sprintf("Component %s - manifest apply returned the following errors:", cn)
			args.l.logAndPrintf("\n%s", cs)
			args.l.logAndPrint("Error: ", out[cn].Err, "\n")
			gotError = true
		}

		if !ignoreError(out[cn].Stderr) {
			args.l.logAndPrint("Error detail:\n", out[cn].Stderr, "\n", out[cn].Stdout, "\n")
			gotError = true
		}
	}

	if gotError {
		args.l.logAndPrint("\n\n✘ Errors were logged during apply operation. Please check component installation logs above.\n")
		return fmt.Errorf("errors were logged during apply operation")
	}
	args.l.logAndPrint("\n\n✔ Installation complete\n")

	crName := installedSpecCRPrefix
	if iops.Revision != "" {
		crName += "-" + iops.Revision
	}
	if err := saveClusterState(iops, crName, opts); err != nil {
		args.l.logAndPrintf("Failed to save install state in the cluster: %s", err)
		return err
	}

	return nil
}

// saveClusterState stores the given IstioOperatorSpec in the cluster as an IstioOperator CR with the given name and
// namespace.
func saveClusterState(iops *v1alpha1.IstioOperatorSpec, name string, opts *kubectlcmd.Options) error {
	iopStr, err := translate.IOPStoIOP(iops, name, iopv1alpha1.Namespace(iops))
	if err != nil {
		return fmt.Errorf("failed to apply manifest with kubectl client: %v", err)
	}
	return manifest.Apply(iopStr, opts)
}
