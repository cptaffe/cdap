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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.GenericData;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import javax.annotation.Nullable;

/**
 * Provides ComputeEngineCredentials either locally if no endpoint is defined, or remotely if endpoint is defined.
 */
public final class ComputeEngineCredentials extends GoogleCredentials {
  private static final Logger LOG = LoggerFactory.getLogger(ComputeEngineCredentials.class);
  private static final Gson GSON = new Gson();
  /**
   * Time (in millisecond) to refresh the credentials before it expires.
   */
  private static final int NUMBER_OF_RETRIES = 10;
  private static final int MIN_WAIT_TIME_MILLISECOND = 500;
  private static final int MAX_WAIT_TIME_MILLISECOND = 10000;
  private static final byte[] lock = new byte[0];
  private static ComputeEngineCredentials computeEngineCredentials;
  private final String endPoint;

  private ComputeEngineCredentials(String endPoint) {
    this.endPoint = endPoint;
  }

  /**
   * Returns ComputeEngineCredentials, and initializes it if doesn't exist.
   *
   * @param endPoint optional endpoint to be used for fetching GoogleCredentials remotely.
   * @return ComputeEngineCredentials
   */
  public static ComputeEngineCredentials createOrGet(@Nullable String endPoint) {
    if (computeEngineCredentials == null) {
      synchronized (lock) {
        if (computeEngineCredentials == null) {
          computeEngineCredentials = new ComputeEngineCredentials(endPoint);
        }
      }
    }

    return computeEngineCredentials;
  }

  private static AccessToken getAccessToken(@Nullable String endPoint) throws IOException {
    if (endPoint != null) {
      return getAccessTokenRemotely(endPoint);
    }

    return getAccessTokenLocally();
  }

  private static AccessToken getAccessTokenLocally() throws IOException {
    try {
      GoogleCredentials googleCredentials = com.google.auth.oauth2.ComputeEngineCredentials.create();
      return googleCredentials.refreshAccessToken();
    } catch (IOException e) {
      throw new IOException("Unable to get credentials from the environment. "
                              + "Please explicitly set the account key.", e);
    }
  }

  private static AccessToken getAccessTokenRemotely(String endPoint) throws IOException {
    ExponentialBackOff backOff = new ExponentialBackOff.Builder()
      .setInitialIntervalMillis(MIN_WAIT_TIME_MILLISECOND)
      .setMaxIntervalMillis(MAX_WAIT_TIME_MILLISECOND).build();

    Exception exception = null;
    int counter = 1;
    while (counter <= NUMBER_OF_RETRIES) {
      try {
        return fetchAccessToken(endPoint);
      } catch (Exception ex) {
        // exception does not get logged since it can be too spammy.
        exception = ex;
      }

      try {
        Thread.sleep(backOff.nextBackOffMillis());
      } catch (InterruptedException ex) {
        exception = ex;
        break;
      }
      counter++;
    }

    LOG.error("Failed to fetch GoogleCredentials after {} retries", counter, exception);
    return null;
  }

  private static AccessToken fetchAccessToken(String endPoint) throws IOException {
    URL url = new URL(endPoint);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.connect();
    try (Reader reader = new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8)) {
      if (connection.getResponseCode() != HttpResponseStatus.OK.code()) {
        throw new IOException(CharStreams.toString(reader));
      }
      GenericData token = GSON.fromJson(reader, GenericData.class);

      String key = token.get("access_token").toString();
      Double expiration = Double.parseDouble(token.get("expires_in").toString());
      long expiresAtMilliseconds = System.currentTimeMillis() +
        expiration.longValue() * 1000;

      return new AccessToken(key, new Date(expiresAtMilliseconds));
    } finally {
      connection.disconnect();
    }
  }

  @Override
  public AccessToken refreshAccessToken() throws IOException {
    return getAccessToken(endPoint);
  }
}
