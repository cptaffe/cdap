/*
 * Copyright © 2015-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.internal.app.deploy.pipeline;

import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.app.deploy.ConfigResponse;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.deploy.InMemoryConfigurator;
import io.cdap.cdap.internal.app.deploy.LocalApplicationManager;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.capability.CapabilityReader;
import io.cdap.cdap.metadata.MetadataValidator;
import io.cdap.cdap.pipeline.AbstractStage;
import io.cdap.cdap.pipeline.Context;
import io.cdap.cdap.pipeline.Pipeline;
import io.cdap.cdap.pipeline.Stage;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import org.apache.twill.filesystem.Location;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * LocalArtifactLoaderStage gets a {@link Location} and emits a {@link ApplicationDeployable}.
 * <p>
 * This stage is responsible for reading the JAR and generating an ApplicationSpecification
 * that is forwarded to the next stage of processing.
 * </p>
 * An active {@link ClassLoader} for the artifact used during deployment will be set to the {@link Context} property
 * with the key {@link LocalApplicationManager#ARTIFACT_CLASSLOADER_KEY}.
 * It is expected a {@link Pipeline#setFinally(Stage)} stage to clean it up after the pipeline execution finished.
 */
public class LocalArtifactLoaderStage extends AbstractStage<AppDeploymentInfo> {
  private static final Gson GSON =
    ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private final CConfiguration cConf;
  private final Store store;
  private final ArtifactRepository artifactRepository;
  private final Impersonator impersonator;
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;
  private final PluginFinder pluginFinder;
  private final CapabilityReader capabilityReader;
  private final MetadataValidator metadataValidator;

  /**
   * Constructor with hit for handling type.
   */
  public LocalArtifactLoaderStage(CConfiguration cConf, Store store, ArtifactRepository artifactRepository,
                                  Impersonator impersonator, AccessEnforcer accessEnforcer,
                                  AuthenticationContext authenticationContext, PluginFinder pluginFinder,
                                  CapabilityReader capabilityReader) {
    super(TypeToken.of(AppDeploymentInfo.class));
    this.cConf = cConf;
    this.store = store;
    this.artifactRepository = artifactRepository;
    this.impersonator = impersonator;
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
    this.pluginFinder = pluginFinder;
    this.capabilityReader = capabilityReader;
    this.metadataValidator = new MetadataValidator(cConf);
  }

  /**
   * Instantiates the Application class and calls configure() on it to generate the {@link ApplicationSpecification}.
   *
   * @param deploymentInfo information needed to deploy the application, such as the artifact to create it from
   *                       and the application config to use.
   */
  @Override
  public void process(AppDeploymentInfo deploymentInfo) throws Exception {

    ArtifactId artifactId = deploymentInfo.getArtifactId();
    Location artifactLocation = deploymentInfo.getArtifactLocation();
    String appClassName = deploymentInfo.getApplicationClass().getClassName();
    String appVersion = deploymentInfo.getApplicationVersion();
    String configString = deploymentInfo.getConfigString();

    EntityImpersonator classLoaderImpersonator =
      new EntityImpersonator(artifactId, impersonator);
    ClassLoader artifactClassLoader = artifactRepository.createArtifactClassLoader(artifactLocation,
                                                                                   classLoaderImpersonator);
    getContext().setProperty(LocalApplicationManager.ARTIFACT_CLASSLOADER_KEY, artifactClassLoader);

    InMemoryConfigurator inMemoryConfigurator = new InMemoryConfigurator(
      cConf, Id.Namespace.fromEntityId(deploymentInfo.getNamespaceId()),
      Id.Artifact.fromEntityId(artifactId), appClassName,
      pluginFinder, artifactClassLoader,
      deploymentInfo.getApplicationName(),
      deploymentInfo.getApplicationVersion(),
      configString);

    ListenableFuture<ConfigResponse> result = inMemoryConfigurator.config();
    ConfigResponse response = result.get(120, TimeUnit.SECONDS);
    if (response.getExitCode() != 0) {
      throw new IllegalArgumentException("Failed to configure application: " + deploymentInfo);
    }
    AppSpecInfo appSpecInfo = GSON.fromJson(response.getResponse(), AppSpecInfo.class);
    ApplicationSpecification specification = appSpecInfo.getAppSpec();
    ApplicationId applicationId;
    if (appVersion == null) {
      applicationId = deploymentInfo.getNamespaceId().app(specification.getName());
    } else {
      applicationId = deploymentInfo.getNamespaceId().app(specification.getName(), appVersion);
    }
    accessEnforcer.enforce(applicationId, authenticationContext.getPrincipal(), StandardPermission.CREATE);
    capabilityReader.checkAllEnabled(specification);

    MetadataMutation.Create mutation = null;
    Metadata metadata = appSpecInfo.getMetadata();
    if (!metadata.getTags().isEmpty() || !metadata.getProperties().isEmpty()) {
      MetadataEntity appEntity = applicationId.toMetadataEntity();
      metadataValidator.validateProperties(appEntity, metadata.getProperties());
      metadataValidator.validateTags(appEntity, metadata.getTags());
      mutation = new MetadataMutation.Create(
        appEntity,
        new io.cdap.cdap.spi.metadata.Metadata(MetadataScope.USER, metadata.getTags(), metadata.getProperties()),
        Collections.emptyMap());
    }

    emit(new ApplicationDeployable(deploymentInfo.getArtifactId(), deploymentInfo.getArtifactLocation(),
                                   applicationId, specification, store.getApplication(applicationId),
                                   ApplicationDeployScope.USER, deploymentInfo.getApplicationClass(),
                                   deploymentInfo.getOwnerPrincipal(), deploymentInfo.canUpdateSchedules(),
                                   appSpecInfo.getSystemTables(), mutation));
  }
}
