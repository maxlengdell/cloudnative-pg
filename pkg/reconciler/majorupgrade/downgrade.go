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

package majorupgrade

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/specs"
)

const jobMajorDowngrade = "major-downgrade"

// ReconcileDowngrade handles major version downgrades
func ReconcileDowngrade(
	ctx context.Context,
	c client.Client,
	cluster *apiv1.Cluster,
	instances []corev1.Pod,
	pvcs []corev1.PersistentVolumeClaim,
) (*ctrl.Result, error) {
	requestedMajor, err := cluster.GetPostgresqlMajorVersion()
	if err != nil {
		return nil, err
	}

	if cluster.Status.PGDataImageInfo == nil || requestedMajor >= cluster.Status.PGDataImageInfo.MajorVersion {
		return nil, nil
	}

	primaryNodeSerial, err := getPrimarySerial(pvcs)
	if err != nil || primaryNodeSerial == 0 {
		return nil, err
	}

	if err := registerPhase(ctx, c, cluster, apiv1.PhaseMajorUpgrade,
		fmt.Sprintf("Downgrading cluster from version %v to %v", 
			cluster.Status.PGDataImageInfo.MajorVersion, requestedMajor)); err != nil {
		return nil, err
	}

	job := createMajorDowngradeJobDefinition(cluster, primaryNodeSerial)
	if err := ctrl.SetControllerReference(cluster, job, c.Scheme()); err != nil {
		return nil, err
	}

	if err := c.Create(ctx, job); err != nil {
		return nil, err
	}

	return &ctrl.Result{Requeue: true}, nil
}

func createMajorDowngradeJobDefinition(cluster *apiv1.Cluster, nodeSerial int) *batchv1.Job {
	downgradeCommand := []string{
		"/controller/manager",
		"instance",
		"downgrade",
		"execute",
	}
	job := specs.CreatePrimaryJob(*cluster, nodeSerial, jobMajorDowngrade, downgradeCommand)
	return job
}
