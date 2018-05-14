package com.netflix.fenzo.common;

import java.util.Locale;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A ThreadFactory builder based on <a href="https://github.com/google/guava">Guava's</a> ThreadFactoryBuilder.
 */
public final class ThreadFactoryBuilder {
    private String nameFormat = null;
    private Boolean daemon = null;

    private ThreadFactoryBuilder() {
    }

    /**
     * Creates a new {@link ThreadFactoryBuilder} builder.
     */
    public static ThreadFactoryBuilder newBuilder() {
        return new ThreadFactoryBuilder();
    }

    /**
     * Sets the naming format to use when naming threads ({@link Thread#setName}) which are created
     * with this ThreadFactory.
     *
     * @param nameFormat a {@link String#format(String, Object...)}-compatible format String, to which
     *                   a unique integer (0, 1, etc.) will be supplied as the single parameter. This integer will
     *                   be unique to the built instance of the ThreadFactory and will be assigned sequentially. For
     *                   example, {@code "rpc-pool-%d"} will generate thread names like {@code "rpc-pool-0"},
     *                   {@code "rpc-pool-1"}, {@code "rpc-pool-2"}, etc.
     * @return this for the builder pattern
     */
    public ThreadFactoryBuilder withNameFormat(String nameFormat) {
        this.nameFormat = nameFormat;
        return this;
    }

    /**
     * Sets whether or not the created thread will be a daemon thread.
     *
     * @param daemon whether or not new Threads created with this ThreadFactory will be daemon threads
     * @return this for the builder pattern
     */
    public ThreadFactoryBuilder withDaemon(boolean daemon) {
        this.daemon = daemon;
        return this;
    }

    public ThreadFactory build() {
        return build(this);
    }

    private static ThreadFactory build(ThreadFactoryBuilder builder) {
        final String nameFormat = builder.nameFormat;
        final Boolean daemon = builder.daemon;
        final AtomicLong count = (nameFormat != null) ? new AtomicLong(0) : null;
        return runnable -> {
            Thread thread = new Thread(runnable);
            if (nameFormat != null) {
                thread.setName(format(nameFormat, count.getAndIncrement()));
            }
            if (daemon != null) {
                thread.setDaemon(daemon);
            }
            return thread;
        };
    }

    private static String format(String format, Object... args) {
        return String.format(Locale.ROOT, format, args);
    }
}