(ns konserve-jdbc.core-mysql-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :refer [<!!]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve-jdbc.core :refer [connect-store release delete-store default-c3p0-level]]))

(def db-spec
  {:dbtype "mysql"
   :dbname "config-test"
   :host "localhost"
   :user "alice"
   :password "foo"})

(deftest jdbc-compliance-sync-test
  (let [_ (delete-store db-spec :table "compliance_test" :opts {:sync? true})
        store  (connect-store db-spec :table "compliance_test" :opts {:sync? true :c3p0-log-level :all})]
    (testing "Compliance test with synchronous store"
      (compliance-test store))
    (release store {:sync? true})
    (delete-store db-spec :opts {:sync? true})))

(deftest jdbc-compliance-async-test
  (let [_ (<!! (delete-store db-spec :table "compliance_test" :opts {:sync? false}))
        store (<!! (connect-store db-spec :table "compliance_test" :opts {:sync? false  :c3p0-log-level :rubbish}))]
    (testing "Compliance test with asynchronous store"
      (compliance-test store))
    (is (= default-c3p0-level (-> store :opts :c3p0-log-level)))
    (<!! (release store {:sync? false}))
    (<!! (delete-store db-spec :opts {:sync? false}))))
