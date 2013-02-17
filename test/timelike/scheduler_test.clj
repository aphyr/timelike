(ns timelike.scheduler-test
  (:refer-clojure :exclude [time])
  (:use clojure.test
        timelike.scheduler))

(defn reset-test!
  [f]
  (reset-scheduler!)
  (f)
  (when-not (zero? @all-threads)
    (await-completion))
  (reset-scheduler!))
(use-fixtures :each reset-test!)

(deftest immediate-thread-test
         (let [a (atom 0)]
           (thread
             (swap! a inc)) 
           (await-completion) 
           (is (= 1 @a)))) 

(deftest multithreaded-test
         (let [a (atom 0)]
           (simultaneous
             (dotimes [i 100]
               (thread (swap! a inc)))) 
           (await-completion)
           (is (= 100 @a))))

(deftest simple-sleep-test
         (let [a (atom [])]
           (thread
             (sleep 1)
             (swap! a conj (time))
             (sleep 100)
             (swap! a conj (time)))
           (await-completion)
           (is (= [1 101] @a))))

(deftest interleaved-sleep-test
         (let [x (atom [])]
           (simultaneous
             (thread
               (swap! x conj [:a (time)])
               (sleep 2)
               (swap! x conj [:a (time)])) 
             (thread
               (sleep 1)
               (swap! x conj [:b (time)])
               (sleep 2)
               (swap! x conj [:b (time)])))
           (await-completion)
           (is (= @x [ 
                      [:a 0] 
                      [:b 1] 
                      [:a 2] 
                      [:b 3]]))))

(deftest parallel-sleep-test
         (let [x (atom [])
               y (atom [])
               z (atom [])]
           (simultaneous
             (dotimes [i 1000]
               (thread
                 (is (= (time) 0))
                 (swap! x conj i)

                 (sleep 1)
                 (is (= (time) 1))
                 (swap! y conj i)

                 (sleep 1)
                 (is (= (time) 2))
                 (swap! z conj i)))) 
           (await-completion)

           ; Verify that they are *not* in any particular order
           (is (not= (sort @x) @x))
           (is (not= (sort @y) @y))
           (is (not= (sort @z) @z))
           (is (not= @x @y))
           (is (not= @x @z))
           (is (not= @y @z))))

(deftest future-test
         (let [futures (map #(future* (inc %)) [-5 10 2 -4 3])]
           (is (= (map deref futures) [-4 11 3 -3 4])))
         (await-completion)) 
