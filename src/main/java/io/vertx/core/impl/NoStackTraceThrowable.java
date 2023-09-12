/*
 * Copyright (c) 2011-2019 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */

package io.vertx.core.impl;

import io.vertx.core.VertxException;

/**
 * @deprecated Please use {@link NoStackTraceException} instead. NoStackTraceThrowable extends Throwable, which is generally considered a bad practice.
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
@Deprecated
public class NoStackTraceThrowable extends VertxException {

  public NoStackTraceThrowable (String message, Throwable cause) {
    super(message, cause, true);
  }

  public NoStackTraceThrowable(String message) {
    super(message, null, true);// disable cause too
  }

  public NoStackTraceThrowable(Throwable cause) {
    super(cause, true);
  }

  public NoStackTraceThrowable() {
    super((Throwable) null, true);// disable cause too
  }
}
