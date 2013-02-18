(defproject timelike "0.1.0-SNAPSHOT"
  :description "A virtual thread simulator."
  :url "http://github.com/aphyr/timelike"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :test-selectors {:default (constantly true)
                   :simple :simple
                   :bamboo :bamboo
                   :layered :layered
                   :faulty :faulty
                   :focus :focus}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/math.numeric-tower "0.0.1"]
                 [incanter/incanter-core "1.4.1"]])
