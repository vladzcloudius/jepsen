(ns scylla.collections.set
  "A set backed by a CQL set."
  (:require [clojure [pprint :refer :all]]
            [clojure.tools.logging :refer [info]]
            [jepsen
             [client    :as client]
             [checker   :as checker]
             [generator :as gen]
             [nemesis   :as nemesis]]
            [qbits.alia :as alia]
            [qbits.hayt :refer :all]
            [scylla [client :as c]
                    [db :as db]]))

(defrecord CQLSetClient [tbl-created? conn writec]
  client/Client

  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [_ test]
    (let [s (:session conn)]
      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (alia/execute s (create-keyspace :jepsen_keyspace
                                           (if-exists false)
                                           (with {:replication {:class :SimpleStrategy
                                                                :replication_factor 3}})))
          (alia/execute s (use-keyspace :jepsen_keyspace))
          (alia/execute s (create-table :sets
                                        (if-exists false)
                                        (column-definitions {:id    :int
                                                             :elements    (set-type :int)
                                                             :primary-key [:id]})
                                        (with {:compaction {:class (db/compaction-strategy)}})))
          (alia/execute s (insert :sets
                                  (values [[:id 0]
                                           [:elements #{}]])
                                  (if-exists false)))))))

  (invoke! [_ test op]
    (let [s (:session conn)]
      (c/with-errors op #{:read}
        (alia/execute s (use-keyspace :jepsen_keyspace))
        (case (:f op)
          :add (do
                 (alia/execute s
                               (update :sets
                                       (set-columns {:elements [+ #{(:value op)}]})
                                       (where [[= :id 0]]))
                               {:consistency writec})
                 (assoc op :type :ok))
          :read (do (db/wait-for-recovery 30 s)
                    (let [value (->> (alia/execute s
                                                   (select :sets
                                                           (where [[= :id 0]]))
                                                   {:consistency :all
                                                    :retry-policy c/aggressive-read})
                                     first
                                     :elements
                                     (into (sorted-set)))]
                      (assoc op :type :ok :value value)))))))

  (close! [_ _]
    (c/close! conn))

  (teardown! [_ _]))

(defn cql-set-client
  "A set implemented using CQL sets"
  ([] (->CQLSetClient (atom false) nil :one))
  ([writec] (->CQLSetClient (atom false) nil writec)))



(defn workload
  [opts]
  {:client          (cql-set-client)
   :generator       (->> (range)
                         (map (fn [x] {:type :invoke, :f :add, :value x}))
                         gen/seq)
   :final-generator (gen/once {:type :invoke, :f :read})
   :checker         (checker/set)})