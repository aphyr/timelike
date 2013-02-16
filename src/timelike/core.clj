(ns timelike.core
  (:import (java.util.concurrent ConcurrentSkipListSet
                                 CountDownLatch)))

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


