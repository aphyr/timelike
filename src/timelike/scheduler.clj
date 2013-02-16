(ns timelike.scheduler
  (:use [clojure.stacktrace :only [print-cause-trace]])
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

(def clock 
  "The current time."
  (atom 0))

(def all-threads
  "The current number of unterminated threads."
  (atom 0))

(def active-threads
  "The current number of awake threads."
  (atom 0))

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

(def barrier-id-atom
  (atom 0))

(defn barrier-id
  "Returns a new barrier id"
  []
  (swap! barrier-id-atom inc))

(defn reset-scheduler!
  "Forcibly reset all state."
  []
  (reset! clock 0)
  (reset! active-threads 0)
  (reset! all-threads 0)
  (reset! completed (promise))
  (reset! barrier-id-atom 0)
  (.clear barriers))

(defn time
  "The current virtual time."
  [] 
  @clock)

(defn advance!
  "Advances the clock to the next time barrier, and releases all threads at
  that barrier simultaneously. Returns the number of threads awakened, or nil if there were no remaining tasks."
  ([]
   ; Atomically remove all elements for the next timestamp.
   (when-let [bs (locking barriers
                   (when-let [b1 (.pollFirst barriers)]
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
                         bs))))]

     ; OK, we've got a bunch of barriers. Advance the clock...
     (swap! clock max (first (first bs)))

     ; Mark that we're releasing N threads...
     (swap! active-threads + (count bs))

     ; LET LOOSE THE DOGS OF WAR
     (doseq [[t id latch] bs]
       (.countDown latch))
     
     (count bs))))

(defn sleep-until
  "Block the current thread until time t. If this is the sole remaining thread,
  triggers the advance! to the next scheduled time."
  [t]
  (let [latch (CountDownLatch. 1)]
;    (prn "Sleeping at" (time) "until" t)

    ; Schedule our awakening
    (.add barriers [t (barrier-id) latch])
    
;    (prn "Barriers is now" barriers)

    ; Mark this thread as inactive, and if we're the last one, advance
    (let [active (swap! active-threads dec)]
      (when (zero? active)
;       (prn "I'm the last thread alive; advancing!")
        (advance!)))

    ; And block
    (.await latch)))

(defn sleep
  "Sleep for n seconds."
  [n]
  (sleep-until (+ n (time))))

(defn thread-exit!
  "Called by a thread when it terminates. Decreemnts active-threads and
  all-threads.
  
  If there are no remaining active threads, advances.
  
  If there are no threads left at *all*, delivers the completed promise so that
  any threads awaiting our completion know the simulation is finished."
  []
  (when (zero? (swap! active-threads dec))
    (advance!))
  (when (zero? (swap! all-threads dec))
    (deliver @completed nil)))

(defn thread*
  "The basic thread primitive. Increments all-threads and active-threads, and
  spawns a new thread. That thread invokes f, then decrements active-threads
  and all-threads."
  [f]
  (swap! all-threads inc)
  (swap! active-threads inc)
  (.start (Thread. (fn []
                     (f)
                     (thread-exit!)))))

(defmacro thread
  "The basic threading macro. Starts a new virtual thread which executes all
  forms in the body."
  [& body]
  `(thread*
     (bound-fn []
       (try ~@body
         (catch Throwable t#
           (locking prn
             (print-cause-trace t#)))))))

(defn await-completion
  "Blocks until all threads have completed."
  []
  (deref @completed))
