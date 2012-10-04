(defproject storm/trident-memcached "0.0.3-wip4"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"twitter-maven" "http://maven.twttr.com/"}

  :dependencies [[com.twitter/util-core "5.3.7" :exclusions [com.google.guava/guava]]
                 [com.twitter/util-collection "5.3.7" :exclusions [com.google.guava/guava]]
                 [com.twitter/util-logging "5.3.7" :exclusions [com.google.guava/guava]]
                 [com.twitter/finagle-core "5.3.8" :exclusions [com.google.guava/guava]]
                 [com.twitter/finagle-memcached "5.3.8" :exclusions [com.google.guava/guava]]
                 ]

  :dev-dependencies [[storm "0.8.1"]
                     [org.clojure/clojure "1.4.0"]
                     [com.thimbleware.jmemcached/jmemcached-cli "1.0.0"]
                     ])

