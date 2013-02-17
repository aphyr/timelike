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
                  (let [responses (constant-load n interval req node)]
                    (node (shutdown))
                    responses))]
    (pstats @results) 
    (println)))

(deftest random-test
         (test-node "Random LB"
                    (random-lb :lb
                               (pool pool-size
                                     (exclusive-queue
                                       (constant-server :rails server-time))))))

(deftest rr-test
         (test-node "Round-robin LB"
                    (rr-lb :lb
                           (pool pool-size
                                 (exclusive-queue
                                   (constant-server :rails server-time))))))

(deftest random-rr-test
         (test-node "Random -> 4 RR LBs -> One pool"
                    (let [backends (pool pool-size
                                         (exclusive-queue
                                           (constant-server 
                                             :rails server-time)))]
                      (random-lb :rand
                                 (pool 4
                                       (rr-lb :rr backends))))))

(deftest even-conn-test
         (test-node "Even connections LB"
                    (even-conn-lb :even
                                  (pool pool-size
                                        (exclusive-queue
                                          (constant-server
                                            :rails server-time))))))
