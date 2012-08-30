(defproject storm/trident-memcached "0.0.2-wip3"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}

  :dependencies [[spy/spymemcached "2.8.1"]
                 [com.thimbleware.jmemcached/jmemcached-cli "1.0.0"]
                 ]

  :dev-dependencies [[storm "0.8.1-wip6"]
                     [org.clojure/clojure "1.4.0"]
                     ])

