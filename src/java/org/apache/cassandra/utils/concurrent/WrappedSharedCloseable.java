/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.concurrent;

import java.util.Arrays;

import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;

/**
 * An implementation of SharedCloseable that wraps a normal AutoCloseable,
 * ensuring its close method is only called when all instances of SharedCloseable have been
 */
public abstract class WrappedSharedCloseable extends SharedCloseableImpl
{
    protected AutoCloseable[] wrapped;

    public WrappedSharedCloseable() {
    }

    public WrappedSharedCloseable(final AutoCloseable closeable)
    {
        this(new AutoCloseable[] {closeable});
    }

    public WrappedSharedCloseable(final AutoCloseable[] closeable)
    {
        super(new Tidy(closeable));
        wrapped = closeable;
    }

    public void setWrapped(AutoCloseable closeable) {
        wrapped = new AutoCloseable[] {closeable};
        super.setRef(new Tidy(wrapped));
    }

    static final class Tidy implements RefCounted.Tidy
    {
        final AutoCloseable[] closeable;
        Tidy(AutoCloseable[] closeable)
        {
            this.closeable = closeable;
        }

        public void tidy() throws Exception
        {
            Throwable fail = null;
            for (AutoCloseable c : closeable)
            {
                try
                {
                    c.close();
                }
                catch (Throwable t)
                {
                    fail = merge(fail, t);
                }
            }
            maybeFail(fail);
        }

        public String name()
    {
        return Arrays.toString(closeable);
    }
    }

    protected WrappedSharedCloseable(WrappedSharedCloseable copy)
    {
        super(copy);
        wrapped = copy.wrapped;
    }
}
