/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.deploy;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.app.deploy.ConfigResponse;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.config.CConfigurationCodec;
import io.cdap.cdap.internal.app.deploy.pipeline.ConfiguratorConfig;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.worker.ConfigResponseResult;
import io.cdap.cdap.internal.app.worker.ConfiguratorTask;
import io.cdap.cdap.internal.app.worker.RunnableTaskRequest;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RemoteConfigurator sends a request to another {@link Configurator} running remotely.
 */
public class RemoteConfigurator implements Configurator {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteConfigurator.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(CConfiguration.class, new CConfigurationCodec()).create();
  private final ConfiguratorConfig config;

  private final RemoteClient remoteClientInternal;

  public RemoteConfigurator(ConfiguratorConfig config,
                            DiscoveryServiceClient discoveryServiceClient) {
    this.config = config;
    this.remoteClientInternal = new RemoteClient(discoveryServiceClient, Constants.Service.TASK_WORKER,
                                                 new DefaultHttpRequestConfig(false),
                                                 String.format("%s", Constants.Gateway.INTERNAL_API_VERSION_3));
  }

  public RemoteConfigurator(CConfiguration cConf, NamespaceId appNamespace, ArtifactId artifactId,
                            String appClassName, PluginFinder pluginFinder, ClassLoader artifactClassLoader,
                            String applicationName, String applicationVersion, String configString,
                            DiscoveryServiceClient discoveryServiceClient,
                            Location artifactLocation) {
    this(new ConfiguratorConfig(cConf, appNamespace,
                                artifactId, appClassName,
                                applicationName,
                                applicationVersion,
                                configString, artifactLocation.toURI()),
         discoveryServiceClient);
  }

  @Override
  public ListenableFuture<ConfigResponse> config() {

    String jsonResponse = "";

    try {
      String param = GSON.toJson(config);
      RunnableTaskRequest req = new RunnableTaskRequest(ConfiguratorTask.class.getName(), param);
      String reqBody = GSON.toJson(req);

      HttpRequest.Builder requestBuilder = remoteClientInternal.requestBuilder(
        HttpMethod.POST, String.format("/worker/run")).withBody(reqBody);
      HttpResponse response = remoteClientInternal.execute(requestBuilder.build());

      jsonResponse = response.getResponseBodyAsString();
    } catch (Exception ex) {
      LOG.warn(String.format("Exception caught during RemoteConfigurator.config, got json response: %s", jsonResponse),
               ex);
      return Futures.immediateFailedFuture(ex);
    }

    ConfigResponseResult configResponse = GSON.fromJson(jsonResponse, ConfigResponseResult.class);
    if (configResponse.getException() != null) {
      //TODO: Refactor to use RemoteTaskExecutor once PR #13383 is merged
      Exception exception = new Exception(configResponse.getException().getMessage());
      exception.setStackTrace(configResponse.getException().getStackTraces());
      return Futures.immediateFailedFuture(exception);
    }
    return Futures.immediateFuture(configResponse.getConfigResponse());
  }
}
