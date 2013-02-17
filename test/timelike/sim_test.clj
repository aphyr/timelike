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
        throughput (throughput reqs)
        [q0 q5 q95 q99 q1] (quantile latencies :probs [0 0.5 0.95 0.99 1])]
    (println "Total reqs:    " n)
    (println "Selected reqs: " (count reqs)) 
    (println "Throughput:    " (float (* 1000 throughput)) "reqs/s")

    (println "Latency distribution:")
    (println "Min:    " q0)
    (println "Median: " q5)
    (println "95th %: " q95)
    (println "99th %: " q99)
    (println "Max:    " q1)))
 
(def n 100000)
(def interval 1)
(def pool-size 250)

(defn test-node
  [name node]
  (println name)
  (let [results (future*
                  (poisson-load n interval req node))]
    (pstats @results) 
    (println)))

(defn backend
  "A singlethreaded, request-queuing server, with a fixed per-request
  overhead plus an exponentially distributed time to process the request,
  connected by a short network cable."
  []
  (cable 2 
    (exclusive-queue
      (fixed-delay 20
        (exponential-delay 100 
          (server :rails))))))

(defn backends
  "A pool of n backends"
  [n]
  (pool n (backend)))

(deftest random-test
         (test-node "Random LB"
                    (random-lb :lb (backends pool-size))))

(deftest rr-test
         (test-node "Round-robin LB"
                    (rr-lb :lb (backends pool-size))))

(deftest random-even-test
         (test-node "Random -> 10 even LBs -> One pool"
                    (let [backends (backends pool-size)]
                      (random-lb :dist
                        (pool 10 
                          (even-conn-lb :sub backends))))))

(deftest random-even-disjoint-test
         (assert (zero? (mod pool-size 10)))
         (test-node 
           "Random -> 10 even LBs -> 10 disjoint pools"
           (random-lb :dist
                      (pool 10
                        (even-conn-lb :sub
                          (backends (/ pool-size 10)))))))

(deftest even-conn-test
         (test-node "Even connections LB"
                    (even-conn-lb :even (backends pool-size))))
