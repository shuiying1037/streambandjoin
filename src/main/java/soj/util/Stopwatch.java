/*
 * Copyright (C) 2008 The Guava Authors
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
 *
 * NOTE: This code is branched from the Google Guava v16.0.1
 */

package soj.util;

import java.util.concurrent.TimeUnit;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.base.Ticker;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public final class Stopwatch
{
    private final Ticker ticker;
    private boolean isRunning;
    private long elapsedNanos;
    private long startTick;

    public static Stopwatch createUnstarted() {
        return new Stopwatch();
    }

    public static Stopwatch createUnstarted(Ticker ticker) {
        return new Stopwatch(ticker);
    }

    public static Stopwatch createStarted() {
        return new Stopwatch().start();
    }

    public static Stopwatch createStarted(Ticker ticker) {
        return new Stopwatch(ticker).start();
    }

    private Stopwatch() {
        this(Ticker.systemTicker());
    }

    private Stopwatch(Ticker ticker) {
        this.ticker = checkNotNull(ticker, "ticker");
    }

    public boolean isRunning() {
        return isRunning;
    }

    public Stopwatch start() {
        checkState(!isRunning, "This stopwatch is already running.");
        isRunning = true;
        startTick = ticker.read();
        return this;
    }

    public Stopwatch stop() {
        long tick = ticker.read();
        checkState(isRunning, "This stopwatch is already stopped.");
        isRunning = false;
        elapsedNanos += tick - startTick;
        return this;
    }

    public Stopwatch reset() {
        elapsedNanos = 0;
        isRunning = false;
        return this;
    }

    private long elapsedNanos() {
        return isRunning ? ticker.read() - startTick + elapsedNanos
                : elapsedNanos;
    }

    public long elapsed(TimeUnit desiredUnit) {
        return desiredUnit.convert(elapsedNanos(), NANOSECONDS);
    }

    @Override
    public String toString() {
        long nanos = elapsedNanos();

        TimeUnit unit = chooseUnit(nanos);
        double value = (double) nanos / NANOSECONDS.convert(1, unit);

        // Too bad this functionality is not exposed as a regular method call
        return String.format("%.4g %s", value, abbreviate(unit));
    }

    private static TimeUnit chooseUnit(long nanos) {
        if (DAYS.convert(nanos, NANOSECONDS) > 0) {
            return DAYS;
        }
        if (HOURS.convert(nanos, NANOSECONDS) > 0) {
            return HOURS;
        }
        if (MINUTES.convert(nanos, NANOSECONDS) > 0) {
            return MINUTES;
        }
        if (SECONDS.convert(nanos, NANOSECONDS) > 0) {
            return SECONDS;
        }
        if (MILLISECONDS.convert(nanos, NANOSECONDS) > 0) {
            return MILLISECONDS;
        }
        if (MICROSECONDS.convert(nanos, NANOSECONDS) > 0) {
            return MICROSECONDS;
        }
        return NANOSECONDS;
    }

    private static String abbreviate(TimeUnit unit) {
        switch (unit) {
        case NANOSECONDS:
            return "ns";
        case MICROSECONDS:
            return "\u03bcs";
        case MILLISECONDS:
            return "ms";
        case SECONDS:
            return "s";
        case MINUTES:
            return "min";
        case HOURS:
            return "h";
        case DAYS:
            return "d";
        default:
            throw new AssertionError();
        }
    }
}