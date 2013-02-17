(ns timelike.scheduler
  (:refer-clojure :exclude [time future])
  (:use [clojure.stacktrace :only [print-cause-trace]] 
        clojure.set)
  (:import (java.util.concurrent ConcurrentSkipListSet
                                 CountDownLatch)))

; Time
;
; The current virtual time is available from the (time) function. Calling
; (sleep n) sleeps for n seconds. No virtual time elapses otherwise. The
; scheduler ensures that memory side effects will proceed in order.
;
; Concurrency
;
; This system includes its own virtual thread scheduler, backed by JVM threads.
; Only when all (virtual) threads have blocked may sleeping threads be awakened
; to proceed. At that time, the clock advances to the next scheduled time and
; awakens any pending threads. There are no guarantees of memory effect
; ordering for threads which awaken at the same wall-clock time, but threads
; scheduled for earlier times happen-before threads scheduled for later times.
; 
; If no two threads are scheduled to awaken at the same time particular time,
; that time in the system will be essentially singlethreaded.
;
; Lifecycle
;
; You can only run one simulation at a time. Start a simulation by creating at
; least one thread. Wait for the simulation to complete with
; (await-completion). Then you can reset the state with (reset-scheduler!)

(defn thread-id
  "Returns the current thread id"
  []
  (.getId (Thread/currentThread))) 

(def logger (agent nil))

(defn log-prn
  [_ & things]
  (apply prn things))

(defn prn*
  "Synchronized prn"
  [& a]
  (apply send-off logger log-prn (thread-id) a))

(def clock 
  "The current time."
  (atom 0))

(def all-threads
  "The current number of unterminated threads."
  (atom 0))

(def active-threads
  "A set of the currently active threads."
  (ref #{}))

(def locks-waiting
  "A map of thread ids to objects which that thread is trying to aquire a lock
  for."
  (ref {}))

(def locks-held
  "A map of objects to the threads which currently hold their lock."
  (ref {}))

(def completed
  "A reference to a promise which is fulfilled once all threads have exited."
  (atom (promise)))

(def barriers
  "A mutable set of [time, id, latch] vectors. Latches are CountdownLatches
  which block threads; they are released when the scheduler reaches that time."
  (ConcurrentSkipListSet.
    (fn [[t1 id1 l1] [t2 id2 l2]]
      (compare [t1 id1]
               [t2 id2]))))

(def thread-count
  (atom 0))

(defn reset-scheduler!
  "Forcibly reset all state."
  []
  (locking barriers
    (dosync
      (assert (empty? (ensure active-threads))) 
      (assert (zero? (deref all-threads))) 
      (assert (.isEmpty barriers)) 
      (assert (empty? (ensure locks-waiting))) 
      (assert (empty? (ensure locks-held)))
      (reset! thread-count 0)
      (reset! clock 0) 
      (reset! completed (promise)) 
      (reset! all-threads 0) 
      (ref-set active-threads #{})
      (ref-set locks-waiting {})
      (ref-set locks-held {})
      (.clear barriers))))

(defn time
  "The current virtual time."
  [] 
  @clock)

(defn can-advance?
  "If all active threads are waiting to acquire a lock held by a different
  thread, we may proceed to the next time."
  []
  (dosync
    (or (empty? (ensure active-threads))
        (let [held    (ensure locks-held)
              waiting (ensure locks-waiting)
              active  @active-threads
              blocked (set (keep (fn [[waiter obj]]
                                   (when-let [holder (held obj)]
                                     (when (not= holder waiter)
                                       waiter)))
                                 waiting))]
          (every? blocked active)))))

(defn advance!
  "Advances the clock to the next time barrier, and releases all threads at
  that barrier. Returns the number of threads awakened, or nil
  if there were no remaining tasks. Advance is always synchronized. May be
  blocked if another thread is within a (simultaneous) block.
  
  At the next timestep, advance may awaken some threads which cannot proceed
  because they are waiting to acquire locks. After all pending barriers have
  been released, advance! checks for this deadlock condition and, if all
  threads are stuck, can advance again, until at least one thread is alive."
  ([]
   (locking barriers
     (loop []
       ; Atomically remove all elements for the next timestamp.
       (when-let [bs (when-let [b1 (.pollFirst barriers)]
                       ; There *is* a next element.
                       (loop [bs (list b1)]
                         (if-let [b (.pollFirst barriers)]
                           (if (<= (first b) (first b1))
                             ; Keep going
                             (recur (conj bs b))
                             ; Whoops, went too far, replace element
                             (do
                               (.add barriers b)
                               bs))
                           ; Nothing left
                           bs)))]

         ; OK, we've got a bunch of barriers. Advance the clock...
         (swap! clock max (first (first bs)))

         ; Mark that we're releasing these threads...
         (dosync
           (alter active-threads union (set (map second bs))))

         ; LET LOOSE THE DOGS OF WAR
         (doseq [[t id latch] bs]
           (.countDown latch))

         ; Check for deadlock at this time and continue if necessary.
         (if (can-advance?)
           (recur)
           (count bs)))))))

(defn inactivate!
  "Called when a thread goes inactive. May trigger advance!"
  []
  (when (dosync
          (alter active-threads disj (thread-id))
          (can-advance?))
    (advance!))
  )

(defn activate!
  "Called when a thread goes active."
  []
  (dosync
    (alter active-threads conj (thread-id))))

(defmacro simultaneous
  "Ensures that the scheduler does not advance! for the duration of the body.
  Use to start multiple threads at once, and prevent one from racing ahead
  until all have a chance to sleep or exit."
  [& body]
  `(locking barriers ~@body))

(defn sleep-until
  "Block the current thread until time t. If this is the sole remaining thread,
  triggers the advance! to the next scheduled time."
  [t]
  (let [latch (CountDownLatch. 1)]
    ; Schedule our awakening. Note that because ConcurrentSkipListMap is only
    ; weakly consistent, we need a lock to guarantee our insertion will be
    ; visible to any advancing threads. I should figure out a faster way to do
    ; this.
    (locking barriers
      (.add barriers [t (thread-id) latch]))

    ; Mark this thread as inactive (and possibly advance)
    (inactivate!)

    ; And block
    (.await latch)
    ))

(defn sleep
  "Sleep for n seconds."
  [n]
  (sleep-until (+ n (time))))

(defn thread-exit!
  "Called by a thread when it terminates. Removes the thread from
  active-threads and all-threads.
  
  If there are no remaining active threads, advances.
  
  If there are no threads left at *all*, delivers the completed promise so that
  any threads awaiting our completion know the simulation is finished."
  []
  (inactivate!)
  (when (zero? (swap! all-threads dec))
    (deliver @completed nil)))

(defn thread*
  "The basic thread primitive. Increments all-threads and active-threads, and
  spawns a new thread. That thread invokes f, then decrements active-threads
  and all-threads."
  [f]
  (swap! all-threads inc)

  ; We construct a temporary thread ID to prevent the scheduler from advancing
  ; until our thread has registered itself as active.
  (let [tmp-thread-id (str "newborn-" (swap! thread-count inc))]
    (dosync (alter active-threads conj tmp-thread-id))
    (.start (Thread. (fn [] 
                       (.setName (Thread/currentThread) 
                                 (str "timelike " (thread-id)))
                       (activate!)
                       (dosync (alter active-threads disj tmp-thread-id))
                       (f)
                       (thread-exit!))))))

(defmacro thread
  "The basic threading macro. Starts a new virtual thread which executes all
  forms in the body."
  [& body]
  `(thread*
     (bound-fn []
       (try ~@body
         (catch Throwable t#
           (send-off logger (fn [_#]
                              (print-cause-trace t#))))))))

(defn await-completion
  "Blocks until all threads have completed."
  []
  (deref @completed))

(defmacro locking*
  "Just like clojure's (locking): acquires a lock on x, then executes body,
  then releases the lock. locking* differs in that it *also* allows the
  scheduler to advance time when threads are blocked on a lock. If you don't
  use locking*, a thread which acquires a lock then goes to sleep could prevent
  other threads from completing: a deadlock.
  
  Warning: x *must* have valid hashCode() and equals(...) methods."
  [x & body]
  `(let [lockee# ~x
         id# (thread-id)]

     ; Signal that we're awaiting the lock.
     (when (dosync
             (alter locks-waiting assoc id# lockee#)
             (can-advance?))
       (advance!))
     
     ; Acquire lock and execute body
     (locking lockee#
       (try
         (dosync
           (alter locks-held assoc lockee# id#)
           (alter locks-waiting dissoc id#)) 
         ~@body

         (finally
           ; Release lock
           (dosync
             (alter locks-held dissoc lockee# id#)))))))

(defmacro future*
  "Analogous to Clojure's future, but works with the virtual thread scheduler.
  Returns a promise immediately. Spawns a thread to execute body, and delivers
  the last value of the body to the promise when the thread completes."
  [& body]
  `(let [p# (promise)]
     (thread
       (deliver p#
                (do ~@body)))
     p#))

(defn deref*
  "Analogous to deref, but marks this thread as inactive so long as it's
  blocked. Allows the scheduler to proceed."
  [x]
  (inactivate!)
  (try
    (deref x)
    (finally
      (activate!))))
