(defproject storm/trident-memcached "0.0.5-wip3"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :repositories {"twitter-maven" "http://maven.twttr.com/"}

  :dependencies [[com.twitter/util-core "5.3.7" :exclusions [com.google.guava/guava]]
                 [com.twitter/util-collection "5.3.7" :exclusions [com.google.guava/guava]]
                 [com.twitter/util-logging "5.3.7" :exclusions [com.google.guava/guava]]
                 [com.twitter/finagle-core "5.3.8" :exclusions [com.google.guava/guava]]
                 [com.twitter/finagle-memcached "5.3.8" :exclusions [com.google.guava/guava]]
                 [com.thimbleware.jmemcached/jmemcached-cli "1.0.0"]
                 ]

  :profiles {
      :provided {
      :dependencies [[storm "0.9.0-wip15"]
                     [org.clojure/clojure "1.4.0"]
                     ]}}
  )
