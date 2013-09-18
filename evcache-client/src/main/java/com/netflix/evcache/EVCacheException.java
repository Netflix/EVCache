/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.evcache;


/**
 * Base exception class for any error conditions that occur while using an EVCache client
 * to make service calls to EVCache Server.
 */
public class EVCacheException extends RuntimeException {

    private static final long serialVersionUID = -3885811159646046383L;

    /**
     * Constructs a new EVCacheException with the specified message.
     *
     * @param message Describes the error encountered.
     */
    public EVCacheException(String message) {
        super(message);
    }

    /**
     * Constructs a new EVCacheException with the specified message and exception indicating the root cause.
     *
     * @param message Describes the error encountered.
     * @param cause The root exception that caused this exception to be thrown.
     */
    public EVCacheException(String message, Throwable cause) {
        super(message, cause);
    }
}
