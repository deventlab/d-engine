(ns jepsen.d_engine.bank
  "Bank workload: concurrent transfers under faults must preserve total balance.
   Encodes 3 account balances as a single packed u64 for atomic CAS."
  (:require [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [clojure.java.shell :as shell]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [slingshot.slingshot :refer [try+]]))

(def bank-key     1)
(def n-accounts   3)
(def init-balance 1000)

;; Pack 3 balances (21 bits each) into a single u64.
(defn pack [a b c]
  (bit-or (bit-shift-left (long a) 42)
          (bit-shift-left (long b) 21)
          (long c)))

(defn unpack [packed]
  [(bit-and (bit-shift-right packed 42) 0x1FFFFF)
   (bit-and (bit-shift-right packed 21) 0x1FFFFF)
   (bit-and packed 0x1FFFFF)])

(def init-packed (pack init-balance init-balance init-balance))

(defn ctl! [cmd endpoints & args]
  (let [result (apply shell/sh cmd "--endpoints" endpoints args)]
    (if (zero? (:exit result))
      (str/trim (:out result))
      (throw (ex-info "ctl failed" {:err (:err result) :exit (:exit result)})))))

(defn parse-long-safe [s]
  (when (and s (not (str/includes? s "not found")))
    (try (Long/parseLong (str/trim s))
         (catch NumberFormatException _ nil))))

(defrecord BankClient [cmd endpoints]
  client/Client

  (open! [this test node] this)

  (setup! [this test]
    (ctl! cmd endpoints "put" (str bank-key) (str init-packed)))

  (invoke! [this test op]
    (try+
      (case (:f op)
        :read
        (let [packed   (or (parse-long-safe (ctl! cmd endpoints "lget" (str bank-key)))
                           init-packed)
              balances (unpack packed)]
          (assoc op :type :ok :value (zipmap (range n-accounts) balances)))

        :transfer
        (let [{:keys [from to amount]} (:value op)]
          (loop []
            (let [packed   (or (parse-long-safe (ctl! cmd endpoints "lget" (str bank-key)))
                               init-packed)
                  bals     (vec (unpack packed))
                  from-bal (nth bals from)]
              (if (< from-bal amount)
                (assoc op :type :fail :error :insufficient-funds)
                (let [new-bals   (-> bals (update from - amount) (update to + amount))
                      new-packed (apply pack new-bals)
                      swapped    (ctl! cmd endpoints "cas"
                                       (str bank-key) (str packed) (str new-packed))]
                  (if (= swapped "true")
                    (assoc op :type :ok)
                    (recur))))))))

      (catch Exception e
        (assoc op :type :info :error (.getMessage e)))))

  (teardown! [this test])
  (close! [this test]))

(defn bank-checker []
  (let [expected (* n-accounts init-balance)]
    (reify checker/Checker
      (check [_ test history opts]
        (let [bad (->> history
                       (filter #(and (= :ok (:type %)) (= :read (:f %))))
                       (remove #(= expected (reduce + (vals (:value %))))))]
          {:valid?    (empty? bad)
           :bad-reads (take 10 bad)})))))

(defn r [_ _] {:type :invoke :f :read :value nil})
(defn t [_ _]
  {:type :invoke :f :transfer
   :value {:from   (rand-int n-accounts)
           :to     (rand-int n-accounts)
           :amount (inc (rand-int 5))}})

(defn workload [opts]
  {:client    (BankClient. (:command opts) (:endpoints opts))
   :checker   (bank-checker)
   :generator (gen/mix [t r])})
