(defproject storm/trident-memcached "0.0.1-SNAPSHOT"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"twitter-maven" "http://maven.twttr.com/", "storm" "https://clojars.org/storm"}

  :dependencies [[spy/spymemcached "2.8.1"]
                 [com.thimbleware.jmemcached/jmemcached-cli "1.0.0"]
                 [com.twitter/util-core "5.3.7"]
                 [com.twitter/util-collection "5.3.7"]
                 [com.twitter/util-logging "5.3.7"]
                 [com.twitter/finagle-core "5.3.8"]
                 [com.twitter/finagle-memcached "5.3.8"]
                 [storm "0.8.0"]
                 [org.clojure/clojure "1.4.0"]
                 ]

  :dev-dependencies [[storm "0.8.0"]
                     [org.clojure/clojure "1.4.0"]
                     ])

