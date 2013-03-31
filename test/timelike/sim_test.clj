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
 
(def n 100000)
(def interval 1.5)
(def pool-size 250)

(defn test-node
  [name node]
  (println name)
  (let [results (future*
                  (load-poisson n interval req node))]
    (pstats @results) 
    (println)))

(defn dyno
  "A singlethreaded, request-queuing server, with a fixed per-request
  overhead plus an exponentially distributed time to process the request,
  connected by a short network cable."
  []
  (cable 2 
    (queue-exclusive
      (delay-fixed 20
        (delay-exponential 100 
          (server :rails))))))

(defn faulty-dyno
  "Like a dyno, but only 90% available."
  []
  (cable 2
    (faulty 20000 1000
      (queue-exclusive
        (delay-fixed 20
          (delay-exponential 100
            (server :rails)))))))

(defn dynos
  "A pool of n dynos"
  [n]
  (pool n (dyno)))

(defn unicorn-worker
  "A pool of n unicorn workers."
  [n]
  (cable 2
    (lb-min-conn
      (worker-pool n
        (mutex
          (delay-fixed 20
            (delay-exponential 100
              (server :rails))))))))

(defn unicorn-master
  "A pool of n unicorn master with x number of workers"
  [n x]
  (pool n
    (unicorn-worker x)))

(deftest single-dyno-test
         (let [responses (future*
                           (load-poisson 10000 150 req (dyno)))]
           (println "A single dyno")
           (println)
           (prn (first @responses))
           (pstats @responses)
           (println)))

(deftest ^:simple random-test
         (test-node "Random LB"
           (lb-random 
             (dynos pool-size))))

(deftest ^:simple rr-test
         (test-node "Round-robin LB"
           (lb-rr 
             (dynos pool-size))))

(deftest ^:simple min-conn-test
         (test-node "Min-conn LB"
           (lb-min-conn
             (dynos pool-size))))

(deftest ^:simple unicorn-test
         (test-node "Unicorn LB"
           (lb-min-conn
             (unicorn-master (/ pool-size 2) 2))))

(defn bamboo-test
  [n]
  (test-node (str "Bamboo with " n " routers")
     (let [dynos (dynos pool-size)]
       (lb-random
         (pool n
           (cable 5
             (lb-min-conn
               dynos)))))))

(deftest ^:bamboo bamboo-2
         (bamboo-test 2))
(deftest ^:bamboo bamboo-4
         (bamboo-test 4))
(deftest ^:bamboo bamboo-8
         (bamboo-test 8))
(deftest ^:bamboo bamboo-16
         (bamboo-test 16))

(deftest ^:faulty min-conn-faulty-test
         (test-node "Min-conn -> pool of faulty dynos."
           (lb-min-conn
             (pool pool-size
               (faulty-dyno)))))

(deftest ^:faulty min-conn-faulty-test-hold
         (test-node "Min-conn with 1s error hold time -> pool of faulty dynos."
           (lb-min-conn :lb {:error-hold-time 1000}
             (pool pool-size
               (faulty-dyno)))))

(deftest ^:faulty retry-min-conn-faulty-test
         (test-node "Retry -> min-conn -> faulty pool"
           (retry 3
             (lb-min-conn :lb {:error-hold-time 1000}
               (pool pool-size
                 (faulty-dyno))))))

(defn faulty-lb
  [pool]
  (faulty 20000 1000
    (retry 3
      (lb-min-conn :lb {:error-hold-time 1000}
        pool))))

(deftest ^:distributed random-faulty-lb-test
  (test-node "Random -> 10 faulty lbs -> One pool"
    (let [dynos (pool pool-size (faulty-dyno))]
      (lb-random
        (pool 10
          (cable 5
            (faulty-lb
              dynos)))))))

(deftest ^:distributed retry-random-faulty-lb-test
  (test-node "Retry -> Random -> 10 faulty lbs -> One pool"
    (let [dynos (pool pool-size (faulty-dyno))]
      (retry 3
        (lb-random
          (pool 10
            (cable 5
              (faulty-lb
                dynos))))))))

(deftest ^:distributed retry-random-faulty-lb-block-test
  (assert (zero? (mod pool-size 10)))
  (test-node "Retry -> Random -> 10 faulty lbs -> 10 pools"
    (retry 3
      (lb-random
        (pool 10
          (cable 5
            (faulty-lb
              (pool (/ pool-size 10)
                (faulty-dyno)))))))))
