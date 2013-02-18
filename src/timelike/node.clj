(ns timelike.node
  (:refer-clojure :exclude [time future])
  (:import (java.util.concurrent ConcurrentSkipListSet
                                 CountDownLatch
                                 LinkedBlockingQueue
                                 LinkedTransferQueue
                                 TimeUnit))
  (:use timelike.scheduler
        clojure.math.numeric-tower
        [incanter.distributions :only [draw exponential-distribution]]))

; A component in this system takes a request and returns a response. Both
; request and response are lists of maps. The history of a particular request
; is encoded, from oldest to newest, in the list. A history threaded through
; this pipeline might look like:
;
; [load balancer] -> [server]    
;                    [server] -> [db]
;                    [server] <- [db]
; [load balancer] <- [server]
; 
; ({:node "load balancer in" :time 0}
;  {:node "server in" :time 1}
;  {:node "db" :time 2}
;  {:node "server out" :time 2}
;  {:node "load balancer out" :time 3})
;  
; A node is an function which accepts a request object and returns a history.

(defn shutdown
  "A special shutdown request."
  []
  [{:time (time) :shutdown true}]) 

(defn shutdown?
  "Does this request mean shut down?"
  [req]
  (:shutdown (first req)))

(defn error
  "Merges {:time time :error true} with m"
  ([] (error {}))
  ([m] (merge {:time (time) :error true} m)))

(defn error?
  "Is the most recent event in the response an error?"
  [req]
  (:error (last req)))

(defmacro try-req
  "Takes a body returning a response. If the response is an error, binds that
  error to err in (catch err ...), and evaluates catch expressions in order
  instead, returning the last return value of the last catch expression."
  [& forms]
  (let [catch-exprs (map rest (filter #(and (list? %)
                                            (= 'catch (first %)))
                                      forms))
        body        (filter #(or (not (list? %))
                                 (not= 'catch (first %)))
                            forms)
        response    (gensym 'response)]
    `(let [~response (do ~@body)]
       (if (error? ~response)
         (do ~@(map (fn [[sym & body]]
                      `(let [~sym ~response]
                         ~@body))
                    catch-exprs))
         ~response))))

(defn retry
  "Wraps a downstream node; retries requests n times on errors."
  [n downstream]
  (assert (< 0 n))
  (fn [req]
    (loop [i 1
           req req]
        (try-req
          (downstream req)
          (catch err
            (if (<= n i)
              err
              (recur (inc i)
                     (conj err {:retry i 
                                :time (time)}))))))))

(defn delay-fixed
  "Sleeps for dt seconds, then calls downstream."
  [dt downstream]
  (fn [req]
    (sleep dt)
    (downstream req)))

(defn delay-exponential
  "Sleeps for an exponential number of seconds, then calls downstream. Mean is
  the average time to delay, or 1/rate, or 1/lambda. All times rounded."
  [mean downstream]
  (let [dist (exponential-distribution (/ mean))]
    (fn [req]
      (sleep (round (draw dist)))
      (downstream req))))

(defn cable
  "A network cable with fixed latency of dt seconds in both directions."
  [dt downstream]
  (fn [req]
    (sleep dt)
    (let [res (downstream req)]
      (sleep dt)
      res)))

(defn mutex
  "Returns a transparent mutexed node which ensures requests are processed one
  at a time--but makes no ordering guarantees."
  [downstream]
  (let [lock (lock)]
    (fn [req]
      (locking* lock
        (downstream req)))))

(defn queue-exclusive
  "Wraps a node in a queue which can only process one message at a time. Each
  call to this node enters a queue; the thread blocks until its turn arrives,
  and then it calls (downstream req)."
  [downstream]
  (let [queue (LinkedBlockingQueue.)]
    (fn [req]
      (let [latch (CountDownLatch. 1)
            pair  [(thread-id) latch]]

        ; LMAO if you are smart enough to do this with CAS memory effects only
        (when-not (locking queue
                    (.put queue pair)
                    (= pair (.peek queue)))
          ; We're not the first. GO TO SLEEEP.
          (inactivate!)
          (.await latch))

        ; Execute request.
        (let [res (downstream req)]

          ; We're at the head of the queue; remove ourselves
          ; and check for a successor.
          (when-let [pair2 (locking queue
                             (assert (= pair (.poll queue)))
                             (.peek queue))]
              ; Activate our successor and allow them to continue.
              (activate! (first pair2))
              (.countDown (second pair2)))

          res)))))

(defn server
  "A node which returns a response."
  ([] (server :server))
  ([name]
   (fn [req]
     (conj req {:node name :time (time)}))))

(defn faulty
  "A node which toggles between OK and failure modes. In its failure state, it
  returns errors instead of passing requests downstream. The time spent in each
  state is exponentially distributed, with the mean time before failure being
  the first argument, and mean time to resolution being the second."
  [mean-uptime mean-downtime downstream]
  (let [up-dist   (exponential-distribution   (/ mean-uptime))
        down-dist (exponential-distribution (/ mean-downtime))
        ; [are we online, next time to transition at]
        state     (atom [true (draw up-dist)])]
    (fn [req]
      ; State transition?
      (let [[up? _] (swap! state (fn [[up? t :as state]]
                                   (if (< (time) t)
                                     state
                                     (if up?
                                       [false (+ (time) (draw down-dist))]
                                       [true  (+ (time) (draw up-dist))]))))]
        (if up?
          (downstream req)
          (conj req (error)))))))

(defmacro pool
  "Evaluates body n times and returns a vector of the results."
  [n & body]
  `(mapv 
     (fn [i#] ~@body)
     (range ~n)))

(defn lb
  "A load balancer. Takes a node name and a function which returns a backend,
  and uses that function to distribute requests to backends."
  [name claim-fn release-fn]
  (fn [req]
    ((claim-fn) (conj req {:node name :time (time)}))))

(defn lb-random
  "A random load balancer. Takes a pool and distributes requests to a randomly
  selected member."
  ([pool] (lb-random :lb-random pool))
  ([name pool]
   (lb name 
       #(nth pool (rand (count pool)))
       identity)))

(defn lb-rr
  "A round-robin load balancer. Takes a pool and distributes subsequent
  requests to subsequent backends."
  ([pool] (lb-rr :lb-rr pool))
  ([name pool]
   (let [i (atom 0)]
     (lb name 
         (fn []
           (nth pool 
                (swap! i #(mod (inc %) (count pool)))))
         identity))))

(defn lb-min-conn
  "A load balancer which tries to evenly distribute connections over backends."
  ([pool] (lb-min-conn :lb-minn-conn pool))
  ([name pool]
  (let [conns (atom (apply sorted-set
                           (map (fn [idx] [0 idx])
                                (range (count pool)))))
        ; Grab a connection.
        acquire (fn acquire []
                  (let [a (atom nil)]
                    (swap! conns
                           (fn acquire-swap [conns]
                             (let [[count idx :as conn] (first conns)
                                   conns (-> conns
                                           (disj conn)
                                           (conj [(inc count) idx]))] 
                               (reset! a idx)
                               conns)))
                    @a))
        
        ; Release a connection.
        release (fn release [idx]
                  ; For reasonably loaded clusters, it's probably faster to
                  ; just iterate through the possible conn values at O(k * log
                  ; n) vs linear search at O(n)
                  (swap! conns
                         (fn release-swap [conns]
                           (let [conn (first 
                                        (filter (comp (partial = idx) second)
                                                conns))]
                             (assert conn)
                             (-> conns
                               (disj conn)
                               (conj [(dec (first conn)) idx]))))))]
    (fn [req]
      (let [idx (acquire)
            backend (nth pool idx)
            response (backend req)]
        (release idx)
        response)))))

(defn load-interval
  "Every (dt) seconds, for a total of n requests, fires off a thread to apply
  (req) to node. Returns a list of results."
  [n dt req-generator node]
  (loop [i  0
         ps []]
    (if (< i n)
      (let [p  (promise)
            ps (conj ps p)]
        ; Execute request in a new thread
        (thread
          (let [r (node (req-generator))]
            (when (zero? (mod i 1000)) 
              (print ".")
              (flush))
            (deliver p r)))
        (sleep (dt))
        (recur (inc i) ps))
      (do
        (doall (map deref* ps))))))

(defn load-constant
  "Every dt seconds, for a total of n requests, fires off a thread to apply req
  to node. Returns a list of results."
  [n dt req-generator node]
  (load-interval n (constantly dt) req-generator node))

(defn load-poisson
  "A Poisson-distributed process: requests are uniformly distributed through
  time and independent of each other. Fires off threads to apply (req) to the
  given node. The average rate lambda is 1/mean."
  [n mean req-generator node]
  (let [dist (exponential-distribution (/ mean))]
    (load-interval n #(round (draw dist)) req-generator node)))

(defn req
  "Create a request."
  []
  [{:time (time)}])

(defn first-time
  "When did this request originate?"
  [req]
  (:time (first req)))

(defn last-time
  "When was this request completed?"
  [req]
  (apply max (map :time req)))

(defn latency 
  "The difference between the request's first time and the maximum time"
  [req]
  (- (last-time req)
     (:time (first req))))

(defn response-rate
  "The mean throughput of a sequence of requests, as defined by the latest
  times."
  [reqs]
  (let [finishes (map last-time reqs)
        t0       (apply min finishes)
        t1       (apply max finishes)
        dt       (- t1 t0)]
    (/ (count reqs) dt)))

(defn request-rate
  "The mean throughput of a sequence of requests, as defined by the earliest
  times."
  [reqs]
  (let [starts (map first-time reqs)
        t0     (apply min starts)
        t1     (apply max starts)
        dt     (- t1 t0)]
    (/ (count reqs) dt)))
