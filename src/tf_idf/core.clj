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

(defn sbuild-tuple [line]
  (let [[id content] (string/split line #":" 2)]
    (spark/tuple id content)))

(defn do-search [content pattern]
  (and 
    (not= content nil)
    (not= (re-find pattern content) nil)))

;(defn do-search [content pattern]
;  (not= (re-find pattern content) nil))

(defn xsearch [source pattern]
  (->> source
       (spark/map-to-pair sbuild-tuple)
       (spark/filter (s-de/fn [(id content)] (do-search content pattern)))
       (spark/map (s-de/fn [(id content)] id))
       spark/collect))

(defn find-files [site]
  (let [shard (rem site 10)
        dir (clojure.java.io/file (str "/storage/si-policy/pages/" shard "/site-" site))
        files (file-seq dir)]
    (->> files
         (filter (fn [file] (.endsWith (.getName file) ".html.gz")))
         (map (fn [file] (.getAbsolutePath file))))))

(defn build-rrds [sc site] do
  (->> site
       find-files
       (map #(spark/text-file sc %1))
       (reduce spark/union)))

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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Basic term handling functions

(def stopwords #{"a" "all" "and" "any" "are" "is" "in" "of" "on"
                 "or" "our" "so" "this" "the" "that" "to" "we"})

(defn terms [content]
  (map string/lower-case (string/split content #" ")))

(def remove-stopwords (partial remove (partial contains? stopwords)))


(remove-stopwords (terms "A quick brown fox jumps"))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; tf / idf / tf*idf functions

(defn idf [doc-count doc-count-for-term]
  (Math/log (/ doc-count (+ 1.0 doc-count-for-term))))


(System/getenv "SPARK_LOCAL_IP")


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Basic Spark management

;(defn make-spark-context []
;  (let [c (-> (conf/spark-conf)
;              (conf/master "local[*]")
;              (conf/app-name "tfidf"))]
;    (spark/spark-context c)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Basic data model generation functions
(defn term-count-from-doc
  "Returns a stopword filtered seq of tuples of doc-id,[term term-count doc-terms-count]"
  [doc-id content]
  (let [terms (remove-stopwords
                (terms content))
        doc-terms-count (count terms)
        term-count (frequencies terms)]
    (map (fn [term] (spark/tuple [doc-id term] [(term-count term) doc-terms-count]))
         (distinct terms))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Spark Transformations / Actions

(defn document-count [documents]
  (spark/count documents))

; (term-count-from-doc "doc1" "A quick brown fox")


(defn term-count-by-doc-term [documents]
  (->>
    documents
    (spark/flat-map-to-pair
      (s-de/key-value-fn term-count-from-doc))
    spark/cache))

(defn document-count-by-term [document-term-count]
  (->> document-term-count
       (spark/map-to-pair (s-de/key-value-fn
                            (fn [[_ term] [_ _]] (spark/tuple term 1))))
       (spark/reduce-by-key +)))

(defn idf-by-term [doc-count doc-count-for-term-rdd]
  (spark/map-values (partial idf doc-count) doc-count-for-term-rdd))

(defn tf-by-doc-term [document-term-count]
  (spark/map-to-pair (s-de/key-value-fn
                       (fn [[doc term] [term-count doc-terms-count]]
                         (spark/tuple term [doc (/ term-count doc-terms-count)])))
                     document-term-count))


(defn tf-idf-by-doc-term [doc-count document-term-count term-idf]
  (->> (spark/join (tf-by-doc-term document-term-count) term-idf)
       (spark/map-to-pair (s-de/key-val-val-fn
                            (fn [term [doc tf] idf]
                              (spark/tuple [doc term] (* tf idf)))))
       ))


(defn tf-idf [corpus]
  (let [doc-count (document-count corpus)
        document-term-count (term-count-by-doc-term corpus)
        term-idf (idf-by-term doc-count (document-count-by-term document-term-count))]
    (tf-idf-by-doc-term doc-count document-term-count term-idf)))

(def tuple-swap (memfn ^scala.Tuple2 swap))

(def swap-key-value (partial spark/map-to-pair tuple-swap))

(defn sort-by-value [rdd]
  (->> rdd
       swap-key-value
       (spark/sort-by-key compare false)
       swap-key-value
       ))



(defn -main [& args]
  (let [sc (make-spark-context)]
    (xsearch (build-rrds sc 277097) #"yoga")))

