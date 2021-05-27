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

package io.cdap.cdap.master.spi.twill;

import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;

/**
 * Extension interface for {@link TwillPreparer} to implement if it supports execution of a {@link TwillRunnable}
 * under secure contexts.
 */
public interface SecureTwillPreparer extends TwillPreparer {
  /**
   * Runs the given runnable as a certain identity.
   *
   * @param runnableName name of the {@link TwillRunnable}
   * @param identity the identity name to run as
   * @return this {@link TwillPreparer}
   */
  SecureTwillPreparer withIdentity(String runnableName, String identity);

  /**
   * Runs the given runnable with an externally-provided secret.
   *
   * @param secretName the external secret identifier to attach to the runnables
   * @return this {@link TwillPreparer}
   */
  SecureTwillPreparer withExternalSecret(String... secretName);
}
