(ns konserve-jdbc.core-h2-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :refer [<!!]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve-jdbc.core :refer [connect-jdbc-store delete-store]]
            [konserve-jdbc.util :refer [with-dir]]))

(use-fixtures :once (partial with-dir "./tmp/h2"))

(def db-spec
  {:dbtype "h2"
   :dbname "./tmp/h2/konserve;DB_CLOSE_ON_EXIT=FALSE"
   :user "sa"
   :password ""})

(deftest jdbc-compliance-sync-test
  (let [_ (delete-store db-spec :table "compliance_test"  :opts {:sync? true})
        store  (connect-jdbc-store db-spec :table "compliance_test" :opts {:sync? true})]
    (testing "Compliance test with synchronous store"
      (compliance-test store))
    (delete-store db-spec :opts {:sync? true})))

(deftest jdbc-compliance-async-test
  (let [_ (<!! (delete-store db-spec :table "compliance_test" :opts {:sync? false}))
        store (<!! (connect-jdbc-store db-spec :table "compliance_test" :opts {:sync? false}))]
    (testing "Compliance test with asynchronous store"
      (compliance-test store))
    (<!! (delete-store db-spec :opts {:sync? false}))))
