/*
 * Tencent is pleased to support the open source community by making Polaris available.
 *
 * Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package cn.polarismesh.polaris.sync.taskconfig.autoconfig;

import cn.polarismesh.polaris.sync.extension.taskconfig.TaskConfigProvider;
import cn.polarismesh.polaris.sync.extension.registry.RegistryCenter;
import cn.polarismesh.polaris.sync.extension.report.ReportHandler;
import cn.polarismesh.polaris.sync.taskconfig.config.SyncConfigProperties;
import cn.polarismesh.polaris.sync.taskconfig.config.TaskConfigProviderManager;
import cn.polarismesh.polaris.sync.taskconfig.config.SyncRegistryProperties;
import cn.polarismesh.polaris.sync.registry.server.RegistrySyncServer;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@EnableConfigurationProperties({SyncRegistryProperties.class, SyncConfigProperties.class})
@Configuration(proxyBeanMethods = false)
public class SyncTaskConfigBootstrapConfiguration {

    @Bean(initMethod = "init", destroyMethod = "destroy")
    @ConditionalOnMissingBean
    public TaskConfigProviderManager providerManager(List<TaskConfigProvider> providers, SyncRegistryProperties registryProperties, SyncConfigProperties configProperties) {
        return new TaskConfigProviderManager(providers, registryProperties, configProperties);
    }

    @Bean(initMethod = "init", destroyMethod = "destroy")
    @ConditionalOnMissingBean
    public RegistrySyncServer registrySyncServer(
            TaskConfigProviderManager providerManager, List<RegistryCenter> registryCenters,
            List<ReportHandler> reportHandlers) {
        return new RegistrySyncServer(providerManager, registryCenters, reportHandlers);
    }
}
