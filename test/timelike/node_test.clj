(ns timelike.node-test
  (:refer-clojure :exclude [time future])
  (:use clojure.test
        [clojure.pprint :only [pprint]]
        timelike.scheduler
        timelike.node
        [incanter.stats :only [quantile]]))

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
    (println "Latency distribution:")
    (println "Min:    " q0)
    (println "Median: " q5)
    (println "95th %: " q95)
    (println "99th %: " q99)
    (println "Max:    " q1)))

(deftest random-test
         (let [node (random-lb :lb
                               (pool 10
                                     (singlethreaded
                                       (constant-server :rails 200))))
               results (future*
                         (constant-load 200 1 req node))]
           (pstats @results)))
