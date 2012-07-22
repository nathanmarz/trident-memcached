(defproject trident-memcached "0.0.1-SNAPSHOT"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}

  :dependencies [[spy/spymemcached "2.8.1"]
                 [com.thimbleware.jmemcached/jmemcached-cli "1.0.0"]
                 ]

  :dev-dependencies [[storm "0.8.0-SNAPSHOT"]
                     [storm/trident "0.0.2-SNAPSHOT"]
                     [org.clojure/clojure "1.4.0"]
                     ])

