package edu.brown.utils;

import java.util.Observable;

import org.apache.log4j.Logger;

import edu.brown.utils.LoggerUtil.LoggerBoolean;

/**
 * 
 * @author pavlo
 */
public class ProfileMeasurement {
    public static final Logger LOG = Logger.getLogger(ProfileMeasurement.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * The profile type
     */
    private final String type;
    /**
     * Total amount of time spent processsing the profiled seciton (in ms) 
     */
    private transient long think_time;
    /**
     * This marker is used to set when the boundary area of the code
     * we are trying to profile starts and stops. When it is zero, the
     * system is outside of the profiled area. 
     */
    private transient Long think_marker; 

    private transient int invocations = 0;
    
    private transient boolean reset = false;
    
    /**
     * Constructor
     * @param pmtype
     */
    public ProfileMeasurement(String pmtype) {
        this.type = pmtype;
        this.reset();
    }

    public synchronized void reset() {
        if (this.think_marker != null) {
            this.reset = true;
        }
        this.think_time = 0;
        this.invocations = 0;
    }
    
    public void clear() {
        this.think_marker = null;
        this.invocations = 0;
        this.think_time = 0;
    }
    
    public void resetOnEvent(EventObservable e) {
        e.addObserver(new EventObserver() {
            @Override
            public void update(Observable o, Object arg) {
                ProfileMeasurement.this.reset();
            }
        });
    }

    /**
     * Get the profile type
     * @return
     */
    public String getType() {
        return type;
    }

    /**
     * Get the total amount of time spent in the profiled area in nanoseconds
     * @return
     */
    public long getTotalThinkTime() {
        return (this.think_time);
    }
    /**
     * Get the total amount of time spent in the profiled area in milliseconds
     * @return
     */
    public double getTotalThinkTimeMS() {
        return (this.think_time / 1000000d);
    }
    
    /**
     * Get the average think time per invocation in nanoseconds
     * @return
     */
    public double getAverageThinkTime() {
        return (this.invocations > 0 ? this.think_time / (double)this.invocations : 0d);
    }
    /**
     * Get the average think time per invocation in milliseconds
     * @return
     */
    public double getAverageThinkTimeMS() {
        return (this.getAverageThinkTime() / 1000000d);
    }
    
    /**
     * Get the total number of times this object was started
     * @return
     */
    public int getInvocations() {
        return (this.invocations); 
    }
    
    // ----------------------------------------------------------------------------
    // START METHODS
    // ----------------------------------------------------------------------------

    /**
     * Main method for stop this ProfileMeasurement from recording time
     * @return this
     */

    public synchronized ProfileMeasurement start(long timestamp) {
        assert(this.think_marker == null) : this.type + " - " + this.hashCode();
        if (debug.get()) LOG.debug(String.format("START %s", this));
        this.think_marker = timestamp;
        this.invocations++;
        return (this);
    }
    
    public ProfileMeasurement start() {
        return (this.start(getTime()));
    }
    
    public boolean isStarted() {
        return (this.think_marker != null);
    }

    // ----------------------------------------------------------------------------
    // STOP METHODS
    // ----------------------------------------------------------------------------

    /**
     * Main method for stop this ProfileMeasurement from recording time
     * We will check to make sure that this handle was started first
     * @return this
     */
    public synchronized ProfileMeasurement stop(long timestamp) {
        if (this.reset) {
            this.reset = false;
            this.think_marker = null;
            return (this);
        }
        if (debug.get()) LOG.debug(String.format("STOP %s", this));
        assert(this.think_marker != null) : this.type + " - " + this.hashCode();
        long added = (timestamp - this.think_marker);
        assert(added >= 0);
        this.think_time += added;
        this.think_marker = null;
//        if (type == Type.JAVA) LOG.info(String.format("STOP %s [time=%d, id=%d]", this.type, added, this.hashCode()));
        return (this);
    }
    
    public ProfileMeasurement stop() {
        return (this.stop(getTime()));
    }

    public boolean isStopped() {
        return (this.think_marker == null);
    }

    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    public void appendTime(ProfileMeasurement other) {
        assert(other != null);
        assert(this.type == other.type);
        this.think_time += other.think_time;
        this.think_marker = other.think_marker;
        this.invocations += other.invocations;
    }
 
    public void addThinkTime(long start, long stop) {
        assert(this.think_marker == null) : this.type;
        this.think_time += (stop - start);
    }

    /**
     * Return the current time in nano-seconds
     * @return
     */
    public static long getTime() {
//      return System.currentTimeMillis();
      return System.nanoTime();
    }
    
    /**
     * Start multiple ProfileMeasurements with the same timestamp
     * @param to_start
     */
    public static void start(ProfileMeasurement...to_start) {
        start(false, to_start);
    }
    
    public static void start(boolean ignore_started, ProfileMeasurement...to_start) {
        long time = ProfileMeasurement.getTime();
        for (ProfileMeasurement pm : to_start) {
            if (ignore_started == false || (ignore_started && pm.isStarted() == false)) pm.start(time);
        } // FOR
    }
    
    /**
     * Stop multiple ProfileMeasurements with the same timestamp
     * @param to_stop
     */
    public static void stop(ProfileMeasurement...to_stop) {
        stop(false, to_stop);
    }
    
    /**
     * Stop multiple ProfileMeasurements with the same timestamp
     * If ignore_stopped is true, we won't stop ProfileMeasurements that already stopped
     * @param ignore_stopped
     * @param to_stop
     */
    public static void stop(boolean ignore_stopped, ProfileMeasurement...to_stop) {
        long time = ProfileMeasurement.getTime();
        for (ProfileMeasurement pm : to_stop) {
            if (ignore_stopped == false || (ignore_stopped && pm.isStopped() == false)) pm.stop(time);
        } // FOR
    }
    
    /**
     * Stop one of the given ProfileMeasurement handles and start the other
     * @param to_stop the handle to stop
     * @param to_start the handle to start
     */
    public static void swap(ProfileMeasurement to_stop, ProfileMeasurement to_start) {
        swap(ProfileMeasurement.getTime(), to_stop, to_start);
    }
    
    public static void swap(long timestamp, ProfileMeasurement to_stop, ProfileMeasurement to_start) {
        if (debug.get()) LOG.debug(String.format("SWAP %s -> %s", to_stop, to_start));
        to_stop.stop(timestamp);
        to_start.start(timestamp);
    }
    

    @Override
    public String toString() {
        return (this.toString(false));
    }
    
    public String toString(boolean verbose) {
        if (verbose) {
            return (String.format("%s[total=%d, marker=%s, invocations=%d]",
                    this.type, this.think_time, this.think_marker, this.invocations));    
        } else {
            return (this.type);
        }
    }
}