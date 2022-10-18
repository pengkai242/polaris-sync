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

package cn.polarismesh.polaris.sync.config.plugins.kubernetes;

import cn.polarismesh.polaris.sync.common.pool.NamedThreadFactory;
import cn.polarismesh.polaris.sync.extension.taskconfig.TaskConfigListener;
import cn.polarismesh.polaris.sync.extension.taskconfig.TaskConfigProvider;
import cn.polarismesh.polaris.sync.registry.pb.RegistryProto.Registry;
import com.google.gson.Gson;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import io.kubernetes.client.util.Watchable;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.options.ListOptions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 基于 Kubernetes ConfigMap 的配置提供者
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
@Component
public class KubernetesTaskConfigProvider implements TaskConfigProvider {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesTaskConfigProvider.class);

    private static final String PLUGIN_NAME = "kubernetes";

    private final Set<TaskConfigListener> listeners = new CopyOnWriteArraySet<>();

    private final ScheduledExecutorService configmapWatchService = Executors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("configmap-watch-worker"));

    private final AtomicReference<Registry> holder = new AtomicReference<>();

    private GenericKubernetesApi<V1ConfigMap, V1ConfigMapList> configMapClient;

    private TaskConfig taskConfig;


    @Override
    public void init(Map<String, Object> options) throws Exception {
        Gson gson = new Gson();
        taskConfig = gson.fromJson(gson.toJson(options), TaskConfig.class);
        LOG.info("[ConfigProvider][Kubernetes] init options : {}", options);

        ApiClient apiClient = null;
        if (taskConfig.hasToken()) {
            LOG.info("[ConfigProvider][Kubernetes] use fromToken to build kubernetes client");
            apiClient = io.kubernetes.client.util.Config.fromToken(getAddress(taskConfig.getAddress()), taskConfig.getToken(),
                    false);
        } else {
            LOG.info("[ConfigProvider][Kubernetes] use default kubernetes client");
            apiClient = io.kubernetes.client.util.Config.defaultClient();
        }

        configMapClient = new GenericKubernetesApi<>(V1ConfigMap.class, V1ConfigMapList.class, "", "v1", "configmaps",
                apiClient);
        startAndWatch();
    }

    @Override
    public void addListener(TaskConfigListener listener) {
        listeners.add(listener);
    }

    @Override
    public Registry getTaskConfig() {
        return holder.get();
    }

    @Override
    public String name() {
        return PLUGIN_NAME;
    }

    @Override
    public void close() {
        configmapWatchService.shutdown();
    }

    private static String getAddress(String address) {
        if (address.startsWith("http://") || address.startsWith("https://")) {
            return address;
        }
        return String.format("https://%s", address);
    }

    public void startAndWatch() throws Exception {
        KubernetesApiResponse<V1ConfigMap> response = configMapClient.get(taskConfig.getNamespace(),
                taskConfig.getConfigmapName());
        handleConfigMap(response.getObject());

        Runnable job = () -> {
            Watchable<V1ConfigMap> watchable = null;
            for (; ; ) {
                try {
                    watchable = configMapClient.watch(taskConfig.getNamespace(), new ListOptions());
                    watchable.forEachRemaining(ret -> {
                        if (!Objects.equals(ret.object.getMetadata().getName(), taskConfig.getConfigmapName())) {
                            return;
                        }
                        handleConfigMap(ret.object);
                    });
                } catch (ApiException e) {
                    LOG.error("[ConfigProvider][Kubernetes] namespace: {} name: {} is empty", taskConfig.getNamespace(),
                            taskConfig.getConfigmapName(), e);
                } finally {
                    IOUtils.closeQuietly(watchable);

                    LOG.info("[ConfigProvider][Kubernetes] try re-watch namespace: {} name: {} is empty",
                            taskConfig.getNamespace(),
                            taskConfig.getConfigmapName());
                }
            }
        };

        configmapWatchService.execute(job);
    }

    private void handleConfigMap(V1ConfigMap configMap) {
        if (Objects.isNull(configMap)) {
            LOG.error("[ConfigProvider][Kubernetes] namespace: {} name: {} not found", taskConfig.getNamespace(),
                    taskConfig.getConfigmapName());
            return;
        }
        Map<String, String> data = configMap.getData();
        if (MapUtils.isEmpty(data)) {
            LOG.error("[ConfigProvider][Kubernetes] namespace: {} name: {} is empty", taskConfig.getNamespace(),
                    taskConfig.getConfigmapName());
            return;
        }

        byte[] ret = data.get(taskConfig.getDataId()).getBytes();
        if (ret.length == 0) {
            LOG.error("[ConfigProvider][Kubernetes] namespace: {} name: {} dataId: {} is empty", taskConfig.getNamespace(),
                    taskConfig.getConfigmapName(), taskConfig.getDataId());
            return;
        }

        LOG.info("[ConfigProvider][Kubernetes] receive new config : {}", new String(ret, StandardCharsets.UTF_8));
        try {
            Registry registry = unmarshal(ret);
            holder.set(registry);

            for (TaskConfigListener listener : listeners) {
                listener.onChange(registry);
            }

        } catch (IOException e) {
            LOG.error("[ConfigProvider][Kubernetes] marshal namespace: {} name: {} dataId: {} ", taskConfig.getNamespace(),
                    taskConfig.getConfigmapName(), taskConfig.getDataId(), e);
        }
    }
}
