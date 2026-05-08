(ns jepsen.d_engine.set
  "Set workload: every acknowledged add must survive faults and appear in reads.
   Encodes a set of integers 0-62 as a u64 bitmask; CAS ensures atomic updates."
  (:require [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [clojure.java.shell :as shell]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [slingshot.slingshot :refer [try+]]))

(def set-key 42)

(defn ctl! [cmd endpoints & args]
  (let [result (apply shell/sh cmd "--endpoints" endpoints args)]
    (if (zero? (:exit result))
      (str/trim (:out result))
      (throw (ex-info "ctl failed" {:err (:err result) :exit (:exit result)})))))

(defn parse-long-safe [s]
  (when (and s (not (str/includes? s "not found")))
    (try (Long/parseLong (str/trim s))
         (catch NumberFormatException _ nil))))

(defn decode-set
  "Decode a u64 bitmask into a set of present element indices."
  [packed]
  (->> (range 63)
       (filter #(not= 0 (bit-and packed (bit-shift-left 1 %))))
       set))

(defrecord SetClient [cmd endpoints]
  client/Client

  (open! [this test node] this)

  (setup! [this test]
    (ctl! cmd endpoints "put" (str set-key) "0"))

  (invoke! [this test op]
    (try+
      (case (:f op)
        :add
        (loop []
          (let [current  (or (parse-long-safe (ctl! cmd endpoints "lget" (str set-key))) 0)
                new-val  (bit-or current (bit-shift-left 1 (long (:value op))))
                swapped  (ctl! cmd endpoints "cas"
                               (str set-key) (str current) (str new-val))]
            (if (= swapped "true")
              (assoc op :type :ok)
              (recur))))

        :read
        (let [packed (or (parse-long-safe (ctl! cmd endpoints "lget" (str set-key))) 0)]
          (assoc op :type :ok :value (decode-set packed))))

      (catch Exception e
        (assoc op :type :fail :error (.getMessage e)))))

  (teardown! [this test])
  (close! [this test]))

(defn add-op [_ _] {:type :invoke :f :add :value (rand-int 30)})
(defn read-op [_ _] {:type :invoke :f :read :value nil})

(defn workload [opts]
  {:client    (SetClient. (:command opts) (:endpoints opts))
   :checker   (checker/set-full)
   :generator (gen/mix [add-op read-op])})
