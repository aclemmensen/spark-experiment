(ns tf-idf.core
  (:require [clojure.string :as string]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.serialization]
            [sparkling.destructuring :as s-de])
  (:gen-class))

(defn make-spark-context []
  (let [c (-> (conf/spark-conf) (conf/master "local[*]") (conf/app-name "wat"))]
    (spark/spark-context c)))

(defn build-tuple [line]
  (let [[id content] (string/split line #":" 2)]
    (spark/tuple id content)))

(defn do-search [content pattern]
  (and 
    (not= content nil)
    (not= (re-find pattern content) nil)))

;(defn do-search [content pattern]
;  (not= (re-find pattern content) nil))

(defn search [source pattern]
  (->> source
       ;(spark/map-to-pair build-tuple)
       (spark/filter (s-de/fn [(id content)] (do-search content pattern)))
       (spark/map (s-de/fn [(id content)] id))
       spark/collect))

(defn path-for [site] 
  (let [shard (rem site 10)]
    (str "/storage/si-policy/pages/" shard "/site-" site)))

(defn files-for [site pattern]
  (->> site
       path-for
       clojure.java.io/file
       file-seq
       (filter (fn [file] (.endsWith (.getName file) pattern)))
       (map (fn [file] (.getAbsolutePath file)))
       sort))

(defn build-rrd [sc files] do
  (->> files
       (map #(spark/text-file sc %1))
       (reduce spark/union)
       (spark/map-to-pair build-tuple)))

(defn filter-rrds [raw changed]
  (->> (spark/cogroup raw changed)
       (spark/map-values (s-de/fn [(-contents -new-contents)]
                           (if (> (count new-contents) 0)
                             (last new-contents)
                             (first contents))))))

(defn build-site-rrd [sc site] do
  (let [raw-files (files-for site ".html")
        raw-rrd (build-rrd sc raw-files)
        changed-files (files-for site ".html-changed")]
    (if (= (count changed-files) 0)
      raw-rrd
      (filter-rrds raw-rrd (spark/cache (build-rrd sc changed-files))))))

(defn get-ids [rrd] do
  (->> rrd
       (spark/map (s-de/fn [(id content)] id))
       spark/collect))

;(def easy (spark/text-file sc "/storage/si-policy/pages/9/site-63599/1.html.gz"))
;(def site (spark/text-file sc "/temp/sites/277097.html"))
;(def site-cp (spark/text-file sc "/temp/sites/1.html.gz"))
;(def full (build-rrds 63599))
;(def dtu (build-rrds 277097))

(defn try-take [source]
  (->> source
       ;(spark/map-to-pair build-tuple)
       ;(spark/map-to-pair (fn [line] (let [[id content] (string/split line #":" 2)] (spark/tuple id content))))
       (spark/filter (fn [line] (not= line nil)))
       (spark/map (fn [line] (let [[id _] (string/split line #":" 2)] id)))
       spark/collect))

;(spark/collect (build-rrds 63599))

;(try-take easy)

;(build-tuple "999999999999:hello")

;(->> (spark/text-file sc "/temp/sites/63599.html")
;     (spark/map count)
;     spark/collect
;     )

(defn -main [& args]
  (let [sc (make-spark-context)]
    (search (build-site-rrd sc 277097) #"yoga")))

