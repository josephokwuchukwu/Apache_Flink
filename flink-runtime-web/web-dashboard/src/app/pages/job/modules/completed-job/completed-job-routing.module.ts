/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { JobCheckpointsComponent } from '@flink-runtime-web/pages/job/checkpoints/job-checkpoints.component';
import { JobConfigurationComponent } from '@flink-runtime-web/pages/job/configuration/job-configuration.component';
import { JobExceptionsComponent } from '@flink-runtime-web/pages/job/exceptions/job-exceptions.component';
import { JobDetailComponent } from '@flink-runtime-web/pages/job/job-detail/job-detail.component';
import { ClusterConfigGuard } from '@flink-runtime-web/pages/job/modules/completed-job/cluster-config.guard';
import { ClusterConfigComponent } from '@flink-runtime-web/pages/job/modules/completed-job/cluster-config/cluster-config.component';
import { JobTimelineComponent } from '@flink-runtime-web/pages/job/timeline/job-timeline.component';

const routes: Routes = [
  {
    path: '',
    component: JobDetailComponent,
    children: [
      {
        path: 'overview',
        loadChildren: () => import('../../overview/job-overview.module').then(m => m.JobOverviewModule),
        data: {
          path: 'overview'
        }
      },
      {
        path: 'timeline',
        component: JobTimelineComponent,
        data: {
          path: 'timeline'
        }
      },
      {
        path: 'exceptions',
        component: JobExceptionsComponent,
        data: {
          path: 'exceptions'
        }
      },
      {
        path: 'checkpoints',
        component: JobCheckpointsComponent,
        data: {
          path: 'checkpoints'
        }
      },
      {
        path: 'configuration',
        component: JobConfigurationComponent,
        data: {
          path: 'configuration'
        }
      },
      {
        path: 'cluster_configuration',
        component: ClusterConfigComponent,
        canActivate: [ClusterConfigGuard],
        data: {
          path: 'cluster_configuration'
        }
      },
      { path: '**', redirectTo: 'overview', pathMatch: 'full' }
    ]
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class CompletedJobRoutingModule {}
