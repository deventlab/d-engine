(defproject jepsen.dengine "0.1.4"
  :description "A Jepsen test for d-engine"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"}
  :main jepsen.d_engine
  :dependencies [[org.clojure/clojure "1.11.3"]
                 [jepsen "0.3.5"]
                 [knossos "0.3.8"]
                 [verschlimmbesserung "0.1.3"]
                 [net.java.dev.jna/jna "5.12.1"]]
  :jvm-opts ["-Djava.awt.headless=true"
             "-server"
             "-Xmx72g"])
