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

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisException;
import io.lettuce.core.output.ValueListOutput;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.DefaultEndpoint;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.pubsub.PubSubOutput;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.resource.ClientResources;
import io.netty.channel.Channel;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Mark Paluch
 */
public class PushEndpoint<K, V> extends DefaultEndpoint {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PushEndpoint.class);
    private static final Set<String> SUBSCRIBE_COMMANDS;
    private final List<RedisPushListener> listeners = new CopyOnWriteArrayList<>();
    private final Set<Wrapper<K>> channels;
    private volatile boolean subscribeWritten = false;

    static {
        SUBSCRIBE_COMMANDS = new HashSet<>(2, 1);

        SUBSCRIBE_COMMANDS.add(CommandType.SUBSCRIBE.name());
        SUBSCRIBE_COMMANDS.add(CommandType.PSUBSCRIBE.name());
    }

    /**
     * Initialize a new instance that handles commands from the supplied queue.
     *
     * @param clientOptions client options for this connection, must not be {@literal null}
     * @param clientResources client resources for this connection, must not be {@literal null}.
     */
    public PushEndpoint(ClientOptions clientOptions, ClientResources clientResources) {

        super(clientOptions, clientResources);

        this.channels = ConcurrentHashMap.newKeySet();
    }

    /**
     * Add a new {@link RedisPushListener listener}.
     *
     * @param listener the listener, must not be {@literal null}.
     */
    public void addListener(RedisPushListener listener) {
        listeners.add(listener);
    }

    /**
     * Remove an existing {@link RedisPushListener listener}..
     *
     * @param listener the listener, must not be {@literal null}.
     */
    public void removeListener(RedisPushListener listener) {
        listeners.remove(listener);
    }

    protected List<RedisPushListener> getListeners() {
        return listeners;
    }

    public boolean hasChannelSubscriptions() {
        return !channels.isEmpty();
    }

    public Set<K> getChannels() {
        return unwrap(this.channels);
    }

    @Override
    public void notifyChannelActive(Channel channel) {
        subscribeWritten = false;
        super.notifyChannelActive(channel);
    }

    @Override
    public <K1, V1, T> RedisCommand<K1, V1, T> write(RedisCommand<K1, V1, T> command) {

        if (!subscribeWritten && SUBSCRIBE_COMMANDS.contains(command.getType().name())) {
            subscribeWritten = true;
        }

        return super.write(command);
    }

    @Override
    public <K1, V1> Collection<RedisCommand<K1, V1, ?>> write(Collection<? extends RedisCommand<K1, V1, ?>> redisCommands) {

        if (!subscribeWritten) {
            for (RedisCommand<?, ?, ?> redisCommand : redisCommands) {
                if (SUBSCRIBE_COMMANDS.contains(redisCommand.getType().name())) {
                    subscribeWritten = true;
                    break;
                }
            }
        }

        return super.write(redisCommands);
    }

    private boolean isSubscribed() {
        return subscribeWritten && hasChannelSubscriptions();
    }

    public void notifyMessage(PushOutput<K, V, V> output) {

        // drop empty messages
        if (output.type() == null || (output.channel() == null && output.get() == null)) {
            return;
        }

        updateInternalState(output);
        try {
            notifyListeners(output);
        } catch (Exception e) {
            logger.error("Unexpected error occurred in RedisPubSubListener callback", e);
        }
    }

    protected void notifyListeners(PushOutput<K, V, V> output) {
        // update listeners
        for (RedisPushListener listener : listeners) {
            switch (output.type()) {
                case message:
                    listener.onPush("message", output.get());
                    break;
                case subscribe:
                    listener.onPush("subscribe", output.get());
                    break;
                case unsubscribe:
                    listener.onPush("unsubscribe", output.get());
                    break;
                case invalidate:
                    listener.onPush("invalidate", output.get());
                    break;
                default:
                    throw new UnsupportedOperationException("Operation " + output.type() + " not supported");
            }
        }
    }

    private void updateInternalState(PushOutput<K, V, V> output) {
        // update internal state
        switch (output.type()) {
            case subscribe:
                channels.add(new Wrapper<>(output.channel()));
                break;
            case unsubscribe:
                channels.remove(new Wrapper<>(output.channel()));
                break;
            default:
                break;
        }
    }

    private Set<K> unwrap(Set<Wrapper<K>> wrapped) {

        Set<K> result = new LinkedHashSet<>(wrapped.size());

        for (Wrapper<K> channel : wrapped) {
            result.add(channel.name);
        }

        return result;
    }

    /**
     * Comparison/equality wrapper with specific {@code byte[]} equals and hashCode implementations.
     *
     * @param <K>
     */
    static class Wrapper<K> {

        protected final K name;

        public Wrapper(K name) {
            this.name = name;
        }

        @Override
        public int hashCode() {

            if (name instanceof byte[]) {
                return Arrays.hashCode((byte[]) name);
            }
            return name.hashCode();
        }

        @Override
        public boolean equals(Object obj) {

            if (!(obj instanceof Wrapper)) {
                return false;
            }

            Wrapper<K> that = (Wrapper<K>) obj;

            if (name instanceof byte[] && that.name instanceof byte[]) {
                return Arrays.equals((byte[]) name, (byte[]) that.name);
            }

            return name.equals(that.name);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [name=").append(name);
            sb.append(']');
            return sb.toString();
        }
    }
}
