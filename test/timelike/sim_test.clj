(ns timelike.sim-test
  (:refer-clojure :exclude [time future])
  (:use clojure.test
        [clojure.pprint :only [pprint]]
        timelike.scheduler
        timelike.node
        [incanter.stats :only [quantile]]))

(defn linesep
  [f]
  (f)
  (println))

(defn reset-test!
  [f]
  (reset-scheduler!)
  (f)
  (when-not (zero? @all-threads)
    (await-completion))
  (reset-scheduler!))

(use-fixtures :each reset-test!)

(defn pstats
  "Print statistics. We examing only the middle half of the request set, to
  avoid measuring ramp-up and draining dynamics."
  [reqs]
  (println)
  (let [n          (count reqs)
        reqs       (->> reqs
                     (drop (/ n 4))
                     (take (/ n 2)))
        latencies  (map latency reqs)
        response-rate (response-rate reqs)
        request-rate  (request-rate reqs)
        [q0 q5 q95 q99 q1] (quantile latencies :probs [0 0.5 0.95 0.99 1])]
    (println "Total reqs:      " n)
    (println "Selected reqs:   " (count reqs)) 
    (println "Successful frac: " (float (/ (count (remove error? reqs))
                                         (count reqs))))
    (println "Request rate:    " (float (* 1000 request-rate))  "reqs/s")
    (println "Response rate:   " (float (* 1000 response-rate)) "reqs/s")

    (println "Latency distribution:")
    (println "Min:    " q0)
    (println "Median: " q5)
    (println "95th %: " q95)
    (println "99th %: " q99)
    (println "Max:    " q1)))
 
(def n 40000)
(def interval 1)
(def pool-size 250)

(defn test-node
  [name node]
  (println name)
  (let [results (future*
                  (load-poisson n interval req node))]
    (pstats @results) 
    (println)))

(defn backend
  "A singlethreaded, request-queuing server, with a fixed per-request
  overhead plus an exponentially distributed time to process the request,
  connected by a short network cable."
  []
  (cable 2 
    (queue-exclusive
      (delay-fixed 20
        (delay-exponential 100 
          (server :rails))))))

(defn backends
  "A pool of n backends"
  [n]
  (pool n (backend)))

(deftest random-test
         (test-node "Random LB"
           (lb-random 
             (backends pool-size))))

(deftest rr-test
         (test-node "Round-robin LB"
           (lb-rr 
             (backends pool-size))))

(deftest min-conn-test
         (test-node "Even connections LB"
           (lb-min-conn
             (backends pool-size))))

(deftest random-even-test
         (test-node "Random -> 10 even LBs -> One pool"
           (let [backends (backends pool-size)]
             (lb-random
               (pool 10 
                 (cable 5
                   (lb-min-conn
                     backends)))))))

(deftest ^:focus random-even-disjoint-test
         (assert (zero? (mod pool-size 10)))
         (test-node 
           "Random -> 10 even LBs -> 10 disjoint pools"
           (lb-random
             (pool 10
               (cable 5
                 (lb-min-conn
                   (backends (/ pool-size 10))))))))

(deftest ^:focus random-faulty-even-disjoint
         (assert (zero? (mod pool-size 10)))
         (test-node "Random -> 10 even (faulty) LBs -> one pool"
           (retry 3
             (lb-random
               (pool 10
                 (cable 5
                   (faulty 100 10
                     (lb-min-conn
                       (backends (/ pool-size 10))))))))))
