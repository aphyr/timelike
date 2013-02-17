(ns timelike.node
  (:refer-clojure :exclude [time future])
  (:import (java.util.concurrent ConcurrentSkipListSet
                                 LinkedBlockingQueue
                                 LinkedTransferQueue
                                 TimeUnit))
  (:use timelike.scheduler
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

(def shutdown?
  "Does this request mean shut down?"
  (comp :shutdown first))

(defn mutex
  "Returns a transparent mutexed node which ensures requests are processed one
  at a time--but makes no ordering guarantees."
  [downstream]
  (let [lock (lock)]
    (fn [req]
      (locking* lock
        (downstream req)))))

(defn exclusive-queue
  "Wraps a node in a queue which can only process one message at a time. Each
  call to this node enqueues a request on the queue, which is processed by a
  worker thread."
  [downstream]
  (let [q (LinkedBlockingQueue.)
        lock (lock)]
    (fn [req]
      (.put q req)
      (locking* lock
        (let [r2 (.poll q)]
          (downstream r2))))))

(defn constant-server
  "A node which sleeps for a fixed number of seconds before returning."
  [name dt]
  (fn [req]
      (sleep dt)
      (conj req {:node name :time (time)})))

(defn exponential-server
  "A node which sleeps for an exponentially distributed number of seconds
  before returning. Mean is the inverse of rate."
  [name mean]
  (let [dist (exponential-distribution (/ mean))]
        (fn [req]
          (sleep (draw dist))
          (conj req {:node name :time (time)}))))

(defmacro pool
  "Evaluates body n times and returns a vector of the results."
  [n & body]
  `(mapv 
     (fn [i#] ~@body)
     (range ~n)))

(defn lb
  "A load balancer. Takes a node name and a function which returns a backend,
  and uses that function to distribute requests to backends."
  [name backend-fn]
  (fn [req]
    ((backend-fn) (conj req {:node name :time (time)}))))

(defn random-lb
  "A random load balancer. Takes a pool and distributes requests to a randomly
  selected member."
  [name pool]
  (lb name #(nth pool (rand (count pool)))))

(defn rr-lb
  "A round-robin load balancer. Takes a pool and distributes subsequent
  requests to subsequent backends."
  [name pool]
  (let [i (atom 0)]
    (lb name (fn []
               (nth pool 
                    (swap! i #(mod (inc %) (count pool))))))))

(defn even-conn-lb
  "A load balancer which tries to evenly distribute connections over backends."
  [name pool]
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
        response))))

(defn interval-load
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

(defn constant-load
  "Every dt seconds, for a total of n requests, fires off a thread to apply req
  to node. Returns a list of results."
  [n dt req-generator node]
  (interval-load n (constantly dt) req-generator node))

(defn poisson-load
  "A Poisson-distributed process: requests are uniformly distributed through
  time and independent of each other. Fires off threads to apply (req) to the
  given node. The average rate lambda is 1/mean."
  [n mean req-generator node]
  (let [dist (exponential-distribution (/ mean))]
    (interval-load n #(draw dist) req-generator node)))

(defn req
  "Create a request."
  []
  [{:time (time)}])

(defn shutdown
  "A special shutdown request."
  []
  [{:time (time) :shutdown true}]) 

(defn latency 
  "The difference between the request's first time and the maximum time"
  [req]
  (- (apply max (map :time req)) 
     (:time (first req))))
