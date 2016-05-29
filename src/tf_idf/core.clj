(ns tf-idf.core
  (:require [clojure.string :as string]
            [clojure.set :as cset]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.serialization]
            [sparkling.destructuring :as s-de])
  (:gen-class))

(defn make-spark-config []
  (-> 
    (conf/spark-conf)
    (conf/master "local[*]")
    (conf/app-name "wat")))

(defn make-spark-context []
    (spark/spark-context (make-spark-config)))

(defn build-tuple [line]
  (let [[id content] (string/split line #":" 2)]
    (spark/tuple id content)))

(defn do-search [content pattern]
  (and 
    (not= content nil)
    (not= (re-find pattern content) nil)))

;(defn do-search [content pattern]
;  (not= (re-find pattern content) nil))

(defn collect-set [source]
  (->> source
       spark/collect
       (into #{})))

(defn search [source pattern]
  (->> source
       (spark/filter (s-de/fn [(id content)] (do-search content pattern)))
       (spark/map (s-de/fn [(id content)] id))
       collect-set))

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

(defn build-rdd [sc file] do
  (->> file
       (spark/text-file sc)
       (spark/map-to-pair build-tuple)))

(defn build-rrds [sc files] do
  (->> files
       (map #(build-rdd sc %1))
       (reduce spark/union)))

(defn filter-rrds [raw changed]
  (->> (spark/cogroup raw changed)
       (spark/map-values (s-de/fn [(-contents -new-contents)]
                           (if (> (count new-contents) 0)
                             (last new-contents)
                             (first contents))))))

;;(defn build-site-rrd [sc site] do
;;  (let [raw-files (files-for site ".html")
;;        raw-rrd (build-rrds sc raw-files)
;;        changed-files (files-for site ".html-changed")]
;;    (if (= (count changed-files) 0)
;;      raw-rrd
;;      (filter-rrds raw-rrd (build-rrds sc changed-files)))))

(defn get-ids [rrd] do
  (->> rrd
       (spark/map (s-de/fn [(id content)] id))
       collect-set))

(defn do-site-search [sc site pattern]
  (let [raw-rrd (build-rrds sc (files-for site ".html"))
        cng-rrd (spark/cache (build-rrds sc (files-for site ".html-changed")))
        raw (search raw-rrd pattern)
        cng (search cng-rrd pattern)
        ids (into #{} (get-ids cng-rrd))]
    (->> cng                    ;; take resulting changed pages
         (cset/difference ids)  ;; remove those from the list of all changed pages
         (cset/difference raw)  ;; then remove that set from the raw results
         (cset/union cng))))    ;; union changed + (raw - ignored) together
(comment
  (def sc (make-spark-context))
  (def sl (build-site-rrd sc 58278))
  (def dt (build-site-rrd sc 2770977))
  (count (do-site-search sc 58278 #"pasning")))

(defn -main [& args]
  (let [sc (make-spark-context)]
    (do-site-search sc 2770977 #"yoga")))

