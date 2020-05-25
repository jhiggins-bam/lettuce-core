/*
 * Copyright 2011-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lettuce.core.push;

import io.lettuce.core.RedisChannelWriter;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.StatefulRedisConnectionImpl;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.protocol.ConnectionWatchdog;
import io.lettuce.core.pubsub.*;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * An thread-safe pub/sub connection to a Redis server. Multiple threads may share one {@link StatefulRedisPushConnectionImpl}
 *
 * A {@link ConnectionWatchdog} monitors each connection and reconnects automatically until {@link #close} is called. All
 * pending commands will be (re)sent after successful reconnection.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 * @author Mark Paluch
 */
public class StatefulRedisPushConnectionImpl<K, V> extends StatefulRedisConnectionImpl<K, V>
        implements StatefulRedisPushConnection<K, V> {

    private final PushEndpoint<K, V> endpoint;

    /**
     * Initialize a new connection.
     *
     * @param endpoint the {@link PubSubEndpoint}
     * @param writer the writer used to write commands
     * @param codec Codec used to encode/decode keys and values.
     * @param timeout Maximum time to wait for a response.
     */
    public StatefulRedisPushConnectionImpl(PushEndpoint<K, V> endpoint, RedisChannelWriter writer, RedisCodec<K, V> codec,
                                           Duration timeout) {

        super(writer, codec, timeout);

        this.endpoint = endpoint;
    }

    /**
     * Add a new listener.
     *
     * @param listener Listener.
     */
    @Override
    public void addListener(RedisPushListener listener) {
        endpoint.addListener(listener);
    }

    /**
     * Remove an existing listener.
     *
     * @param listener Listener.
     */
    @Override
    public void removeListener(RedisPushListener listener) {
        endpoint.removeListener(listener);
    }

    /**
     * Re-subscribe to all previously subscribed channels and patterns.
     *
     * @return list of the futures of the {@literal subscribe} and {@literal psubscribe} commands.
     */
    protected List<RedisFuture<Void>> resubscribe() {

        List<RedisFuture<Void>> result = new ArrayList<>();

//        if (endpoint.hasChannelSubscriptions()) {
//            result.add(async().subscribe(toArray(endpoint.getChannels())));
//        }
//
//        if (endpoint.hasPatternSubscriptions()) {
//            result.add(async().psubscribe(toArray(endpoint.getPatterns())));
//        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private <T> T[] toArray(Collection<T> c) {
        Class<T> cls = (Class<T>) c.iterator().next().getClass();
        T[] array = (T[]) Array.newInstance(cls, c.size());
        return c.toArray(array);
    }

    @Override
    public void activated() {
        super.activated();
        for (RedisFuture<Void> command : resubscribe()) {
            command.exceptionally(throwable -> {
                if (throwable instanceof RedisCommandExecutionException) {
                    InternalLoggerFactory.getInstance(getClass()).warn("Re-subscribe failed: " + command.getError());
                }
                return null;
            });
        }
    }
}
