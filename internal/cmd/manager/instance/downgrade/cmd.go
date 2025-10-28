/*
Copyright © contributors to CloudNativePG, established as
CloudNativePG a Series of LF Projects, LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
*/

package downgrade

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"

	"github.com/cloudnative-pg/machinery/pkg/execlog"
	"github.com/cloudnative-pg/machinery/pkg/fileutils"
	"github.com/spf13/cobra"

	"github.com/cloudnative-pg/cloudnative-pg/pkg/management/postgres"
)

// NewCmd creates the downgrade command
func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "downgrade",
		Short: "Downgrade PostgreSQL major version",
	}

	cmd.AddCommand(newExecuteCmd())
	return cmd
}

func newExecuteCmd() *cobra.Command {
	var pgData string
	var podName string
	var clusterName string
	var namespace string

	cmd := &cobra.Command{
		Use:   "execute",
		Short: "Execute major version downgrade using pg_dump/pg_restore",
		RunE: func(cmd *cobra.Command, _ []string) error {
			instance := postgres.NewInstance().
				WithNamespace(namespace).
				WithPodName(podName).
				WithClusterName(clusterName)
			return executeDowngrade(cmd.Context(), instance, pgData)
		},
	}

	cmd.Flags().StringVar(&pgData, "pg-data", os.Getenv("PGDATA"), "The PGDATA to be downgraded")
	cmd.Flags().StringVar(&podName, "pod-name", os.Getenv("POD_NAME"), "The name of this pod")
	cmd.Flags().StringVar(&namespace, "namespace", os.Getenv("NAMESPACE"), "The namespace")
	cmd.Flags().StringVar(&clusterName, "cluster-name", os.Getenv("CLUSTER_NAME"), "The cluster name")

	return cmd
}

func executeDowngrade(ctx context.Context, instance *postgres.Instance, pgData string) error {
	if pgData == "" {
		return fmt.Errorf("PGDATA not set")
	}

	if err := fileutils.EnsureDirectoryExists(postgres.GetSocketDir()); err != nil {
		return fmt.Errorf("while creating socket directory: %w", err)
	}

	dumpFile := path.Join(pgData, "downgrade_dump.sql")

	// Remove incompatible config files
	if err := os.Remove(path.Join(pgData, "custom.conf")); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove custom.conf: %w", err)
	}
	sedCmd := exec.Command("sed", "-i", "/include.*custom.conf/d", path.Join(pgData, "postgresql.conf"))
	if err := execlog.RunStreaming(sedCmd, "sed"); err != nil {
		return fmt.Errorf("sed failed: %w", err)
	}

	// Start the existing PostgreSQL instance to dump data
	startCmd := exec.Command("pg_ctl", "-D", pgData, "-w", "start")
	if err := execlog.RunStreaming(startCmd, "pg_ctl"); err != nil {
		return fmt.Errorf("pg_ctl start failed: %w", err)
	}

	// Dump all data from the running cluster
	dumpCmd := exec.Command("pg_dumpall", "-f", dumpFile)
	if err := execlog.RunStreaming(dumpCmd, "pg_dumpall"); err != nil {
		exec.Command("pg_ctl", "-D", pgData, "stop").Run()
		return fmt.Errorf("pg_dumpall failed: %w", err)
	}

	// Stop the old instance
	stopCmd := exec.Command("pg_ctl", "-D", pgData, "-w", "stop")
	if err := execlog.RunStreaming(stopCmd, "pg_ctl"); err != nil {
		return fmt.Errorf("pg_ctl stop failed: %w", err)
	}

	// Clean up version-specific SQL
	sedCmd := exec.Command("sed", "-i", "-E", `s/LOCALE_PROVIDER = \w+ |^SET transaction_timeout = 0;| WITH INHERIT TRUE GRANTED BY \w+//`, dumpFile)
	if err := execlog.RunStreaming(sedCmd, "sed"); err != nil {
		return fmt.Errorf("sed failed: %w", err)
	}

	// Backup old data directory
	if err := os.Rename(pgData, pgData+".old"); err != nil {
		return fmt.Errorf("failed to rename PGDATA: %w", err)
	}

	// Initialize new data directory with target version
	initCmd := exec.Command("initdb", "-D", pgData, "--username", "postgres")
	if err := execlog.RunStreaming(initCmd, "initdb"); err != nil {
		return fmt.Errorf("initdb failed: %w", err)
	}

	// Start new instance
	startCmd = exec.Command("pg_ctl", "-D", pgData, "-w", "start")
	if err := execlog.RunStreaming(startCmd, "pg_ctl"); err != nil {
		return fmt.Errorf("pg_ctl start failed: %w", err)
	}

	// Restore data
	restoreCmd := exec.Command("psql", "-f", dumpFile, "postgres")
	if err := execlog.RunStreaming(restoreCmd, "psql"); err != nil {
		exec.Command("pg_ctl", "-D", pgData, "stop").Run()
		return fmt.Errorf("restore failed: %w", err)
	}

	// Stop the new instance
	stopCmd = exec.Command("pg_ctl", "-D", pgData, "-w", "stop")
	if err := execlog.RunStreaming(stopCmd, "pg_ctl"); err != nil {
		return fmt.Errorf("pg_ctl stop failed: %w", err)
	}

	// Clean up
	if err := os.RemoveAll(pgData + ".old"); err != nil {
		return fmt.Errorf("failed to remove old PGDATA: %w", err)
	}

	return nil
}