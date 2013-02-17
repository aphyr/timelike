(ns timelike.node-test
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
  [reqs]
  (let [latencies (map latency reqs)
        [q0 q5 q95 q99 q1] (quantile latencies :probs [0 0.5 0.95 0.99 1])]
    (println "Total reqs:" (count reqs))
    (println "Latency distribution:")
    (println "Min:    " q0)
    (println "Median: " q5)
    (println "95th %: " q95)
    (println "99th %: " q99)
    (println "Max:    " q1)))
 
(def n 10000)
(def interval 1)
(def pool-size 250)
(def server-time 200)

(defn test-node
  [name node]
  (println name)
  (let [results (future*
                  (poisson-load n interval req node))]
    (pstats @results) 
    (println)))

(deftest random-test
         (test-node "Random LB"
                    (random-lb :lb
                               (pool pool-size
                                     (exclusive-queue
                                       (exponential-server :rails server-time))))))

(deftest rr-test
         (test-node "Round-robin LB"
                    (rr-lb :lb
                           (pool pool-size
                                 (exclusive-queue
                                   (exponential-server :rails server-time))))))

(deftest random-even-test
         (test-node "Random -> 10 even LBs -> One pool"
                    (let [backends (pool pool-size
                                         (exclusive-queue
                                           (exponential-server 
                                             :rails server-time)))]
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
                                     (pool (/ pool-size 10)
                                           (exclusive-queue
                                             (exponential-server
                                               :rails server-time))))))))

(deftest even-conn-test
         (test-node "Even connections LB"
                    (even-conn-lb :even
                                  (pool pool-size
                                        (exclusive-queue
                                          (exponential-server
                                            :rails server-time))))))
