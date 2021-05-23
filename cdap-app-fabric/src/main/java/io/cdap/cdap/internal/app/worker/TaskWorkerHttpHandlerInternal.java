/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.AbstractBodyConsumer;
import io.cdap.cdap.common.internal.worker.RunnableTaskContext;
import io.cdap.cdap.logging.gateway.handlers.AbstractLogHttpHandler;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Internal {@link HttpHandler} for Task worker.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/worker")
public class TaskWorkerHttpHandlerInternal extends AbstractLogHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerHttpHandlerInternal.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(BasicThrowable.class, new BasicThrowableCodec()).create();
  private final RunnableTaskLauncher runnableTaskLauncher;
  private final Consumer<String> stopper;
  private final AtomicInteger inflightRequests = new AtomicInteger(0);

  @Inject
  public TaskWorkerHttpHandlerInternal(CConfiguration cConf, Consumer<String> stopper) {
    super(cConf);
    runnableTaskLauncher = new RunnableTaskLauncher(cConf);
    this.stopper = stopper;
  }

  /**
   * Run a task specified by {@link RunnableTaskRequest} in the request body.
   *
   * @param request http request whose body contains {@link RunnableTaskRequest} in json format.
   * @param responder {@link HttpResponder}
   */
  @POST
  @Path("/run")
  public void run(FullHttpRequest request, HttpResponder responder) {
    if (inflightRequests.incrementAndGet() > 1) {
      responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
      return;
    }

    String className = null;
    try {
      RunnableTaskRequest runnableTaskRequest =
        GSON.fromJson(request.content().toString(StandardCharsets.UTF_8), RunnableTaskRequest.class);
      className = runnableTaskRequest.getClassName();
      byte[] response = runnableTaskLauncher.launchRunnableTask(runnableTaskRequest, null);
      responder.sendByteArray(HttpResponseStatus.OK, response, EmptyHttpHeaders.INSTANCE);
    } catch (ClassNotFoundException | ClassCastException ex) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, exceptionToJson(ex), EmptyHttpHeaders.INSTANCE);
    } catch (Exception ex) {
      LOG.error(String.format("Failed to run task %s",
                              request.content().toString(StandardCharsets.UTF_8), ex));
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, exceptionToJson(ex), EmptyHttpHeaders.INSTANCE);
    } finally {
      if (className != null) {
        stopper.accept(className);
      }
    }
  }

  /**
   * Run a task specified by query parameters: {@code className} and {@code param}
   *
   * @param request http request whose body contains {@link RunnableTaskRequest} in json format.
   * @param responder {@link HttpResponder}
   */

  /**
   * Run a task specified by query parameters {@code className} and {@code param}.
   * <p>
   * Request body contains the content of a file uploaded by the caller that is available to the task
   * at runtime via {@link RunnableTaskContext}.
   *
   * @param request {@link HttpRequest} whose body should be a file content.
   * @param responder {@link HttpResponder}
   * @param className classname of the task to run
   * @param param parameter(s) to the task
   * @return {@link BodyConsumer}
   */
  @POST
  @Path("/task")
  public BodyConsumer runTask(HttpRequest request, HttpResponder responder,
                              @QueryParam("className") String className,
                              @QueryParam("param") String param) {
    if (inflightRequests.incrementAndGet() > 1) {
      responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
      return null;
    }

    try {
      // Download file content to local tmp dir.
      final File dest = File.createTempFile("run_task_" + className, "tmp");

      return new AbstractBodyConsumer(dest) {
        @Override
        protected void onFinish(HttpResponder responder, File uploadedFile) {
          try {
            RunnableTaskRequest runnableTaskRequest = new RunnableTaskRequest(className, param);
            byte[] response = runnableTaskLauncher.launchRunnableTask(runnableTaskRequest, dest.toURI());
            responder.sendByteArray(HttpResponseStatus.OK,
                                    response,
                                    EmptyHttpHeaders.INSTANCE);
          } catch (ClassNotFoundException | ClassCastException ex) {
            responder.sendString(HttpResponseStatus.BAD_REQUEST,
                                 exceptionToJson(ex),
                                 EmptyHttpHeaders.INSTANCE);
          } catch (Exception ex) {
            LOG.error(String.format("Failed to run task %s", ex));
            responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, exceptionToJson(ex),
                                 EmptyHttpHeaders.INSTANCE);
          } finally {
            dest.delete();
            stopper.accept(className);
          }
        }

        @Override
        protected void onError(Throwable cause) {
          dest.delete();
          LOG.info("Failed to download file to run task", className);
          responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                               exceptionToJson(new Exception(cause)),
                               EmptyHttpHeaders.INSTANCE);
        }
      };
    } catch (IOException e) {
      LOG.error("Failed to download file to run task", className);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, exceptionToJson(e), EmptyHttpHeaders.INSTANCE);
      stopper.accept(className);
    }
    return null;
  }

  /**
   * Return json representation of an exception.
   * Used to propagate exception across network for better surfacing errors and debuggability.
   */
  private String exceptionToJson(Exception ex) {
    BasicThrowable basicThrowable = new BasicThrowable(ex);
    return GSON.toJson(basicThrowable);
  }
}
