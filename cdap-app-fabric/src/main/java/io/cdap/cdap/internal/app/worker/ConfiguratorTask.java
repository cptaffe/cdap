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

package io.cdap.cdap.internal.app.worker;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.app.deploy.ConfigResponse;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.internal.worker.RunnableTask;
import io.cdap.cdap.common.internal.worker.RunnableTaskContext;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.config.CConfigurationCodec;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.deploy.InMemoryConfigurator;
import io.cdap.cdap.internal.app.deploy.pipeline.ConfiguratorConfig;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * ConfiguratorTask is a RunnableTask for performing the configurator config.
 */
public class ConfiguratorTask implements RunnableTask {
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(CConfiguration.class, new CConfigurationCodec()).create();
  private static final Logger LOG = LoggerFactory.getLogger(ConfiguratorTask.class);

  private final CConfiguration cConf;

  @Inject
  public ConfiguratorTask(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    Injector injector = Guice.createInjector(new ConfiguratorTaskModule(cConf));
    Configurator configurator = injector.getInstance(Configurator.class);
    configurator.run(context);
  }

  /**
   * Configurator performs the configurator config.
   */
  public static class Configurator {
    private final Impersonator impersonator;
    private final PluginFinder pluginFinder;
    private final ArtifactRepository artifactRepository;
    private final LocationFactory locationFactory;

    @Inject
    public Configurator(Impersonator impersonator, PluginFinder pluginFinder,
                        ArtifactRepository artifactRepository, LocationFactory locationFactory) {
      this.impersonator = impersonator;
      this.pluginFinder = pluginFinder;
      this.artifactRepository = artifactRepository;
      this.locationFactory = locationFactory;
    }

    public void run(RunnableTaskContext context) throws Exception {
      ConfiguratorConfig config = GSON.fromJson(context.getParam(), ConfiguratorConfig.class);

      // Getting the pipeline app from appfabric
      LOG.info(String.format("Fetching artifact '%s' from app-fabric to create artifact class loader.",
                             config.getArtifactId().getArtifact()));
      Location artifactLocation = Locations.getLocationFromAbsolutePath(
        locationFactory, config.getArtifactLocationURI().getPath());


      try (InputStream is = artifactRepository.newInputStream(Id.Artifact.fromEntityId(config.getArtifactId()))) {
        Files.copy(is, Paths.get(artifactLocation.toURI()));
      }

      LOG.info(String.format("Successfully fetched artifact '%s'.", config.getArtifactId().getArtifact()));

      EntityImpersonator classLoaderImpersonator =
        new EntityImpersonator(config.getArtifactId(), impersonator);
      ClassLoader artifactClassLoader = artifactRepository.createArtifactClassLoader(artifactLocation,
                                                                                     classLoaderImpersonator);

      Id.Namespace namespaceId = Id.Namespace.from(config.getAppNamespace().getEntityName());
      Id.Artifact artifactId = Id.Artifact
        .from(Id.Namespace.from(config.getArtifactId().getNamespace()), config.getArtifactId().toApiArtifactId());

      InMemoryConfigurator configurator = new InMemoryConfigurator(
        config.getcConf(), namespaceId, artifactId,
        config.getAppClassName(), pluginFinder,
        artifactClassLoader,
        config.getApplicationName(), config.getApplicationVersion(),
        config.getConfigString());

      ListenableFuture<ConfigResponse> future = configurator.config();
      ConfigResponseResult result;

      try {
        result = new ConfigResponseResult(future.get(120, TimeUnit.SECONDS), null);
      } catch (Exception ex) {
        LOG.error("Encountered error while validating pipeline: ", ex);
        result = new ConfigResponseResult(null, ex);
      }

      String json = GSON.toJson(result);
      context.writeResult(json.getBytes(StandardCharsets.UTF_8));
    }
  }
}
