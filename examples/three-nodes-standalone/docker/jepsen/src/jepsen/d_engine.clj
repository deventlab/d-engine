(ns jepsen.d_engine
  (:require
   [clojure.tools.logging :refer :all]
   [clojure.string :as str]
   [verschlimmbesserung.core :as v]
   [jepsen [checker :as checker]
      [cli :as cli]
      [client :as client]
      [control :as c]
      [generator :as gen]
      [independent :as independent]
      [nemesis :as nemesis]
      [tests :as tests]]
   [jepsen.checker.timeline :as timeline]
   [jepsen.control.util :as cu]
   [clojure.java.shell :as shell]
   [jepsen.os :as os]
   [knossos.model :as model]
   [slingshot.slingshot :refer [try+]]
   [clojure.tools.cli :refer [parse-opts]]
   [jepsen.d_engine.bank :as bank]
   [jepsen.d_engine.set :as set-workload]))

;; ========== Operation Definition ==========
;; Update operation commands to match v0.1.4 API
(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn w [_ _] {:type :invoke, :f :write, :value (rand-int 5)})

;; Adjust mixed ratio: 70% linear reads + 30% normal reads
(def mixed-reads (gen/mix [{:weight 7, :gen r} {:weight 3, :gen r}]))

;; ========== Command execution tool ==========
(defn ctl-command
  "Execute the dengine_ctl command and process the output"
  [cmd & args]
  (let [command (concat [cmd] args) ; Ensure everything is a string
        _ (info "Executing command: " (pr-str command))  ; Better logging
        result (apply shell/sh command)] ; Add Rust debugging information
    (println "Executing command:" command)  ;; Log the command being executed
    (info "Command output:" (:out result))
    (info "Command error:" (:err result))
    (if (zero? (:exit result))
      (do
        (println "Success:" (:out result))  ;; Log success output
        (:out result))
      (throw (ex-info "Command failed"
                      {:exit (:exit result)
                       :err (:err result)
                       :out (:out result)})))))

;; ========== Client Implementation ==========
(defn parse-long-nil
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (println "Parsing raw string:" (pr-str s))
  (when s
    (try
      (-> s
          (clojure.string/trim) ; Clean leading and trailing whitespace (including newlines)
          (Long/parseLong))
      (catch NumberFormatException _
        nil)))) ; Return nil if parsing fails

(defn client
  "A client for a single compare-and-set register"
  [cmd endpoints]
  (assert (string? endpoints) (str "ENDPOINTS MUST BE STRING. GOT: " endpoints))
  (reify client/Client

    (open! [this test node]
      (info "Opening client for node:" node)
      this)

    (invoke! [this test op]
      (println "Received operation:" op)
      (let [[k v] (:value op)
            ; Read operation failures are marked as :fail, write operations are marked as :info
            crash-type (if (#{:get :lget} (:f op)) :fail :info)]
        (try+
         (case (:f op)
            ; Linear read (lget)
           :read (let [result (parse-long-nil
                               (ctl-command cmd "--endpoints" endpoints "lget" (str k)))]
                   (if result
                     (assoc op :type :ok, :value (independent/tuple k result))
                     (assoc op :type :fail, :error :not-found)))

; Write operation (put)
           :write (do
                    (println "endpoints:" endpoints "; Putting value:" v "for key:" k)
                    (ctl-command cmd "--endpoints" endpoints "put" (str k) (str v))
                    (assoc op :type :ok)))

; ===== Error handling =====
         (catch java.net.SocketTimeoutException e
           (assoc op :type crash-type, :error :timeout))

         (catch [:exit 4005] e ; cluster not available
           (assoc op :type crash-type :error :cluster-unavailable))

         (catch Exception e
           (let [err-msg (or (.getMessage e)
                             (some-> e ex-data :err)
                             (some-> e ex-data :body))]
             (cond
               (and err-msg (str/includes? (str/lower-case err-msg) "key not found"))
               (assoc op :type :fail :error :not-found)

               (and err-msg (str/includes? (str/lower-case err-msg) "cluster unavailable"))
               (assoc op :type crash-type :error :cluster-unavailable)

               :else
               (assoc op :type crash-type :error (or err-msg "unknown error"))))))))

    (close! [_ _]
      (info "Closing client"))

    (setup! [_ _])
    (teardown! [_ _])))

;; ========== Checker Implementation ==========
(defn split-history
  "Split History: Linear Reads vs. Normal Operations"
  [history]
  (group-by (fn [op] (if (= :lget (:f op)) :lget-ops :other-ops)) history))

(defn checker
  "Linearizability checker"
  [test history opts]
  (independent/checker
   (checker/compose
    {;; :perf   (checker/perf)
     :linear (checker/linearizable {:model     (model/cas-register)
                                    :algorithm :linear})
     :timeline (timeline/html)})))

(defn register-workload
  "Original linearizable register workload."
  [opts]
  {:client    (client (:command opts) (:endpoints opts))
   :checker   (independent/checker
               (checker/compose
                {:linear   (checker/linearizable {:model     (model/cas-register)
                                                  :algorithm :auto})
                 :timeline (timeline/html)}))
   :generator (independent/concurrent-generator
               3
               (range 3)
               (fn [k]
                 (->> (gen/mix [r w])
                      (gen/stagger 1/2)
                      (gen/limit 40))))})

(defn workload
  [opts]
  (case (:workload opts)
    "bank" (bank/workload opts)
    "set"  (set-workload/workload opts)
    (register-workload opts)))

(defn test-spec
  [opts]
  (println "opts:" opts)
  (println "Time limit set to:" (:time-limit opts))
  (let [wl (workload opts)]
    (merge tests/noop-test
           {:name      (str "d-engine-" (:workload opts "register"))
            :ssh       {:private-key-path "/root/.ssh/id_rsa"
                        :strict-host-key-checking false}
            :client    (:client wl)
            :nemesis   (nemesis/partition-random-halves)
            :checker   (:checker wl)
            :generator (->> (:generator wl)
                            (gen/stagger 1/10)
                            (gen/nemesis
                             (cycle [(gen/sleep 10)
                                     {:type :info, :f :start}
                                     (gen/sleep 5)
                                     {:type :info, :f :stop}]))
                            (gen/time-limit (:time-limit opts)))}
           opts)))

(def cli-opts
  "Additional command line options."
  [["-c" "--command CMD" "CLI binary path"
    :default "client-usage-standalone-demo"
    :parse-fn identity
    :validate [(complement empty?) "command cannot be empty."]]
   ["-e" "--endpoints ENDPOINTS" "d-engine gRPC endpoints (comma-separated)"
    :default "http://node1:9080,http://node2:9080,http://node3:9080"
    :parse-fn identity
    :validate [(complement empty?) "endpoints cannot be empty."]]
   ["-w" "--workload NAME" "Workload to run: register (default), bank, set"
    :default "register"
    :parse-fn identity
    :validate [#{"register" "bank" "set"} "must be one of: register, bank, set"]]])

(defn -main
  "handles command lien arguments"
  [& args]
  (cli/run!
   (merge (cli/single-test-cmd {:test-fn test-spec
                                :opt-spec cli-opts})
          (cli/serve-cmd))
   args))
