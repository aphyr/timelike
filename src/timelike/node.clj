(ns timelike.node
  (:refer-clojure :exclude [time future])
  (:use timelike.scheduler))

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

(defn singlethreaded
  "Returns a transparent singlethreaded node which ensures requests are
  processed in order."
  [downstream]
  (let [o (Object.)]
    (fn [req]
      (locking* o
        (downstream req)))))

(defn constant-server
  "A node which sleeps for a fixed number of seconds before returning."
  [node dt]
  (fn [req]
      (sleep dt)
      (conj req {:node node :time (time)})))

(defmacro pool
  "Evaluates body n times and returns a vector of the results."
  [n & body]
  `(mapv 
     (fn [i#] ~@body)
     (range ~n)))

(defn random-lb
  "A random load balancer. Takes a pool and distributes requests to a randomly
  selected member."
  [node pool]
  (fn [req]
    (let [backend (nth pool (rand (count pool)))]
      (backend (conj req {:node node :time (time)})))))

(defn constant-load
  "Every dt seconds, for a total of n requests, fires off a thread to apply req
  to node. Returns a list of results."
  [n dt req-generator node]
  (loop [i  (dec n)
         ps []]
    (let [p (promise)]
      ; Execute request in a new thread
      (thread
        (let [r (node (req-generator))]
          (deliver p r)))

      (if (zero? i)
        (do
          ; Resolve all promises and return.
          (doall (map deref* (conj ps p)))) 
        (do
          ; Next round
          (sleep dt)
          (recur (dec i) (conj ps p)))))))

(defn req
  "Create a request."
  []
  [{:time (time)}])

(defn latency 
  "The difference between the request's first time and the maximum time"
  [req]
  (- (apply max (map :time req)) 
     (:time (first req))))
