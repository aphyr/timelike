# Timelike

An experiment in simulating parallel systems perfectly. Provides a green
threading model and a controlled clock allowing for parallel execution of
arbitrarily many threads. Virtual time advances as fast as the system can go,
which allows you to simulate long-running or very large-scale systems quickly,
without error. 

If a thread says it wants to wake up at time t, it awakes at *exactly* time t,
is guaranteed to see all memory side effects from all threads which ran at
earlier times, and that only threads scheduled to awaken at t are currently
running. The clock *only* advances when threads choose to sleep, allowing you
precise control over the simulated evolution of the system. Naturally, one can
trivially recover stochastic ordering dynamics if desired.

## Usage

```clj
(use 'timelike.scheduler)

(def counter (atom 0))

(dotimes [i 100]
  (thread
    ; All these swaps run concurrently at time 0.
    (swap! counter inc)

    (time) ; returns 0

    ; Blocks for a random amount of time
    (sleep (rand))

    ; The scheduler guarantees that this part of the code executes
    ; one at a time (unless two rands were the same)

    (time) ; might return 0.0023453, or 0.99234.
    (swap! counter inc)

    ; And now let's synchronize: all threads wake up simultaneously at 100 seconds:
    (sleep-until 100)
    (time) ; returns 100
    (swap! counter inc)))

; Blocks until all threads have completed. This should take only a few milliseconds.
(await-completion)

(prn @counter) ; Prints 300: 100 threads performing 3 increments each.
```

## License

Copyright © 2013 Kyle Kingsbury

Distributed under the Eclipse Public License, the same as Clojure.
