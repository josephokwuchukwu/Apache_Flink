/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.security.contexts;

import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityContextInitializeException;

import java.util.concurrent.Callable;

/** Test security context factory class for service provider discovery. */
public class TestSecurityContextFactory implements SecurityContextFactory {

    @Override
    public boolean isCompatibleWith(SecurityConfiguration securityConfig) {
        return true;
    }

    @Override
    public SecurityContext createContext(SecurityConfiguration securityConfig)
            throws SecurityContextInitializeException {
        return new TestSecurityContext();
    }

    /** Test security context class. */
    public static class TestSecurityContext implements SecurityContext {

        @Override
        public <T> T runSecured(Callable<T> securedCallable) throws Exception {
            return null;
        }
    }
}
