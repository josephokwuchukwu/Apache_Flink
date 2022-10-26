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

import { InjectionToken } from '@angular/core';

import { ModuleConfig } from '@flink-runtime-web/core/module-config';
import { flinkEditorOptions } from '@flink-runtime-web/share/common/editor/editor-config';

type routerKeys = 'jobManager';

export type JobManagerModuleConfig = Omit<ModuleConfig<routerKeys>, 'customComponents' | 'routerTabs'>;

export const JOB_MANAGER_MODULE_DEFAULT_CONFIG: Required<JobManagerModuleConfig> = {
  editorOptions: flinkEditorOptions,
  routerFactories: {
    jobManager: (jobManagerName: string) => [jobManagerName]
  }
};

export const JOB_MANAGER_MODULE_CONFIG = new InjectionToken<JobManagerModuleConfig>('job-manager-module-config', {
  providedIn: 'root',
  factory: () => JOB_MANAGER_MODULE_DEFAULT_CONFIG
});
