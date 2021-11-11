(ns konserve-jdbc.util
  (:require [clojure.java.io :as io])
  (:import  [java.io File]))

(defn delete-recursively [filename]
  (let [func (fn [func f]
               (when (.isDirectory ^File f)
                 (doseq [^File f2 (.listFiles ^File f)]
                   (func func f2)))
               (try (io/delete-file f) (catch Exception _ nil)))]
    (func func (io/file filename))))

(defn with-dir [^String dir f]
  (.mkdirs (File. dir))
  (f)
  (delete-recursively dir))
